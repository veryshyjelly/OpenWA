import { WwebjsMessaging, downloadMediaInPage } from './wwebjs-messaging';
import * as downloads from './wwebjs-messaging';
import { WhatsAppWebJsAdapter } from './whatsapp-web-js.adapter';
import { WwebjsEngineHost } from './wwebjs-host';
import { ConcurrencyLimiter } from '../../common/utils/concurrency-limiter';
import { createLogger } from '../../common/services/logger.service';
import { onDemandMediaRead } from '../../common/utils/on-demand-media';

const CHAT = '120363000001@g.us';
const ID = `false_${CHAT}_OLD`;
const bytes = { mimetype: 'image/webp', filename: 'animated.webp', data: 'YWJj', sizeBytes: 3 };
const freshSignal = () => new AbortController().signal;

describe('single-message source media', () => {
  const env = { ...process.env };
  afterEach(() => {
    process.env = { ...env };
    jest.restoreAllMocks();
  });

  function messaging(message: unknown) {
    const downloadMessageMedia = jest.fn().mockResolvedValue(bytes);
    const sendSeen = jest.fn();
    const client = {
      getMessageById: jest.fn().mockResolvedValue(message),
      getChatById: jest.fn().mockResolvedValue(undefined),
      sendSeen,
    };
    const host = {
      getClient: () => client,
      ensureReady: jest.fn(),
      downloadMessageMedia,
      isPageTransportError: () => false,
      reportIfPageTransportError: jest.fn(),
      logger: createLogger('media.spec'),
    } as unknown as WwebjsEngineHost;
    return { reader: new WwebjsMessaging(host), downloadMessageMedia, client };
  }

  it('resolves the exact old message, checks its chat, and never reads the chat or marks it read', async () => {
    const { reader, client, downloadMessageMedia } = messaging({ from: CHAT, hasMedia: true, type: 'sticker' });
    await expect(reader.getMessageMedia(CHAT, ID, 100, freshSignal())).resolves.toEqual(bytes);
    expect(client.getMessageById).toHaveBeenCalledWith(ID);
    expect(downloadMessageMedia).toHaveBeenCalledTimes(1);
    expect(client.getChatById).not.toHaveBeenCalled();
    expect(client.sendSeen).not.toHaveBeenCalled();
  });

  it('retains filenames and MIME metadata without downloading history media', async () => {
    const message = {
      id: { _serialized: ID },
      from: CHAT,
      to: 'me',
      fromMe: false,
      type: 'document',
      timestamp: 100,
      body: 'caption',
      hasMedia: true,
      _data: { mimetype: 'application/pdf', filename: 'original.pdf', size: 123 },
      getContact: jest.fn().mockResolvedValue(undefined),
    };
    const { reader, client, downloadMessageMedia } = messaging(message);
    client.getChatById.mockResolvedValue({ fetchMessages: jest.fn().mockResolvedValue([message]) });
    const rows = await reader.getChatHistory(CHAT, 1000, false);
    expect(rows[0].media).toEqual({
      mimetype: 'application/pdf',
      filename: 'original.pdf',
      sizeBytes: 123,
      omitted: true,
    });
    expect(downloadMessageMedia).not.toHaveBeenCalled();
  });

  it.each([
    null,
    { from: 'other@g.us', hasMedia: true },
    { from: CHAT, hasMedia: false },
    { from: CHAT, hasMedia: true, type: 'revoked' },
  ])('refuses a missing, cross-chat, non-media or revoked message before download (%j)', async message => {
    const { reader, downloadMessageMedia } = messaging(message);
    await expect(reader.getMessageMedia(CHAT, ID, 100, freshSignal())).rejects.toMatchObject({
      response: { code: 'MEDIA_SOURCE_UNAVAILABLE' },
    });
    expect(downloadMessageMedia).not.toHaveBeenCalled();
  });

  it('refuses status/broadcast reads and malformed message ids without a page lookup', async () => {
    const { reader, client } = messaging({ from: CHAT, hasMedia: true });
    await expect(reader.getMessageMedia('status@broadcast', ID, 100, freshSignal())).rejects.toThrow();
    await expect(reader.getMessageMedia(CHAT, 'bare-id', 100, freshSignal())).rejects.toThrow();
    expect(client.getMessageById).not.toHaveBeenCalled();
  });

  function adapter() {
    // Exercise the real strict downloader with the same gates as the adapter; no browser/session.
    const instance = Object.create(WhatsAppWebJsAdapter.prototype) as {
      inboundLimiter: ConcurrencyLimiter;
      ensureReady: jest.Mock;
      downloadMessageMedia(message: unknown, maxBytes: number, signal: AbortSignal): Promise<typeof bytes>;
    };
    instance.inboundLimiter = new ConcurrencyLimiter(1);
    instance.ensureReady = jest.fn();
    const source = jest.spyOn(downloads, 'downloadCappedMedia').mockResolvedValue(bytes);
    return { instance, source };
  }

  it('uses the existing capped page downloader and preserves the media envelope', async () => {
    const { instance, source } = adapter();
    await expect(instance.downloadMessageMedia({ _data: { size: 3 } }, 8, freshSignal())).resolves.toEqual(bytes);
    expect(source).toHaveBeenCalledWith({ _data: { size: 3 } }, 8, true);
  });

  it('respects disabled downloads and pre-gates declared oversize without touching WhatsApp', async () => {
    const { instance, source } = adapter();
    process.env.MEDIA_DOWNLOAD_ENABLED = 'false';
    await expect(instance.downloadMessageMedia({}, 8, freshSignal())).rejects.toMatchObject({ status: 409 });
    process.env.MEDIA_DOWNLOAD_ENABLED = 'true';
    process.env.MEDIA_DOWNLOAD_MAX_BYTES = '4';
    await expect(instance.downloadMessageMedia({ _data: { size: 5 } }, 8, freshSignal())).rejects.toMatchObject({
      status: 413,
    });
    expect(source).not.toHaveBeenCalled();
  });

  it('distinguishes source absence, transient download failure and actual oversize', async () => {
    const { instance, source } = adapter();
    source.mockResolvedValueOnce(undefined);
    await expect(instance.downloadMessageMedia({}, 8, freshSignal())).rejects.toMatchObject({
      response: { code: 'MEDIA_SOURCE_UNAVAILABLE' },
    });
    source.mockRejectedValueOnce(new Error('temporary'));
    await expect(instance.downloadMessageMedia({}, 8, freshSignal())).rejects.toMatchObject({ status: 503 });
    source.mockResolvedValueOnce({ ...bytes, data: '', sizeBytes: 9 });
    await expect(instance.downloadMessageMedia({}, 8, freshSignal())).rejects.toMatchObject({ status: 413 });
  });

  it('holds an active slot past timeout and skips expired queued downloads', async () => {
    process.env.MEDIA_DOWNLOAD_TIMEOUT_MS = '10';
    const { instance, source } = adapter();
    let release!: (value: typeof bytes) => void;
    source.mockImplementationOnce(
      () =>
        new Promise(resolve => {
          release = resolve;
        }),
    );
    await expect(onDemandMediaRead(signal => instance.downloadMessageMedia({}, 8, signal))).rejects.toMatchObject({
      status: 503,
    });
    await expect(onDemandMediaRead(signal => instance.downloadMessageMedia({}, 8, signal))).rejects.toMatchObject({
      status: 503,
    });
    expect(source).toHaveBeenCalledTimes(1);
    release(bytes);
    await new Promise(resolve => setImmediate(resolve));
    await expect(onDemandMediaRead(signal => instance.downloadMessageMedia({}, 8, signal))).resolves.toEqual(bytes);
    expect(source).toHaveBeenCalledTimes(2);
  });

  it('does no work for a cancelled request', async () => {
    const read = jest.fn();
    const abort = new AbortController();
    abort.abort();
    await expect(onDemandMediaRead(read, abort.signal)).rejects.toMatchObject({ status: 503 });
    expect(read).not.toHaveBeenCalled();
  });

  it.each(['REUPLOADING', 'FETCHING'])('keeps %s page state retryable on strict reads', async stage => {
    const message = { mediaData: { mediaStage: stage }, downloadMedia: jest.fn() };
    const target = globalThis as unknown as { window: unknown };
    target.window = { require: () => ({ Msg: { get: () => message } }) };
    try {
      await expect(downloadMediaInPage({ msgId: ID, maxBytes: 100, strict: true })).rejects.toThrow(/retry/);
    } finally {
      delete (target as { window?: unknown }).window;
    }
  });

  it.each(['ERROR_MISSING', 'ERROR_UNKNOWN'])('checks the source after %s and can recover bytes', async stage => {
    const download = jest.fn().mockResolvedValue(new ArrayBuffer(3));
    const message = { mediaData: { mediaStage: stage }, downloadMedia: jest.fn(), mimetype: 'image/webp' };
    const target = globalThis as unknown as { window?: unknown };
    target.window = {
      require: (module: string) =>
        module === 'WAWebCollections'
          ? { Msg: { get: () => message } }
          : { downloadManager: { downloadAndMaybeDecrypt: download } },
      WWebJS: { arrayBufferToBase64Async: jest.fn().mockResolvedValue('YWJj') },
    };
    try {
      await expect(downloadMediaInPage({ msgId: ID, maxBytes: 100, strict: true })).resolves.toMatchObject({
        data: 'YWJj',
        sizeBytes: 3,
      });
      expect(download).toHaveBeenCalledTimes(1);
    } finally {
      delete target.window;
    }
  });

  it.each([404, 503, 429])('uses the source response after ERROR_MISSING (HTTP %s)', async status => {
    const download = jest.fn().mockRejectedValue({ status, name: 'MediaDownloadError' });
    const message = { mediaData: { mediaStage: 'ERROR_MISSING' }, downloadMedia: jest.fn() };
    const target = globalThis as unknown as { window?: unknown };
    target.window = {
      require: (module: string) =>
        module === 'WAWebCollections'
          ? { Msg: { get: () => message } }
          : { downloadManager: { downloadAndMaybeDecrypt: download } },
    };
    try {
      const read = downloadMediaInPage({ msgId: ID, maxBytes: 100, strict: true });
      if (status === 404) await expect(read).resolves.toBeUndefined();
      else await expect(read).rejects.toThrow(`status ${status}`);
      expect(download).toHaveBeenCalledTimes(1);
    } finally {
      delete target.window;
    }
  });
});
