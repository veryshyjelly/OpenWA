import { EventEmitter } from 'events';
import type { Response } from 'express';
import { ServiceUnavailableException } from '@nestjs/common';
import { CHAT_SCOPED_KEY } from '../auth/decorators/auth.decorators';
import { MessageController } from './message.controller';
import { MessageService } from './message.service';
import type { BulkMessageService } from './bulk-message.service';
import type { ChatScopeService } from '../auth/chat-scope.service';

describe('explicit source media API', () => {
  const env = { ...process.env };
  afterEach(() => {
    process.env = { ...env };
  });
  function service() {
    const getMessageMedia = jest
      .fn()
      .mockResolvedValue({ data: 'YWJj', mimetype: 'application/pdf', filename: 'résumé.pdf' });
    const requireEngine = jest.fn().mockReturnValue({ getMessageMedia });
    const instance = Object.create(MessageService.prototype) as MessageService;
    Object.assign(instance, { engines: { require: requireEngine } });
    return { instance, getMessageMedia, requireEngine };
  }

  it('reads exactly one engine message with session/chat identity and tightens the server cap', async () => {
    process.env.MEDIA_DOWNLOAD_MAX_BYTES = '10';
    const { instance, getMessageMedia, requireEngine } = service();
    const media = await instance.downloadChatMedia('session', 'chat@g.us', 'message', 20);
    expect(requireEngine).toHaveBeenCalledWith('session', expect.any(Function));
    expect(getMessageMedia).toHaveBeenCalledWith('chat@g.us', 'message', 10, expect.any(AbortSignal));
    expect(media).toMatchObject({
      size: 3,
      mimetype: 'application/octet-stream',
      sourceMimetype: 'application/pdf',
      filename: 'résumé.pdf',
    });
    const chunks = [];
    for await (const chunk of media.stream) chunks.push(chunk);
    expect(Buffer.concat(chunks).toString()).toBe('abc');
  });

  it.each([0, -1, NaN, Infinity, 1.5])('rejects invalid cap %s before engine lookup', async max => {
    const { instance, requireEngine } = service();
    await expect(instance.downloadChatMedia('s', 'c', 'm', max)).rejects.toMatchObject({ status: 400 });
    expect(requireEngine).not.toHaveBeenCalled();
  });

  it('defends the engine boundary against oversized bytes and bounds even a stuck lookup', async () => {
    const { instance, getMessageMedia } = service();
    await expect(instance.downloadChatMedia('s', 'c', 'm', 1)).rejects.toMatchObject({ status: 413 });
    process.env.MEDIA_DOWNLOAD_TIMEOUT_MS = '10';
    getMessageMedia.mockImplementationOnce(() => new Promise(() => undefined));
    await expect(instance.downloadChatMedia('s', 'c', 'm')).rejects.toMatchObject({ status: 503 });
  });

  it('answers 501 for an engine without this capability', async () => {
    const { instance, requireEngine } = service();
    requireEngine.mockReturnValue({});
    await expect(instance.downloadChatMedia('s', 'c', 'm')).rejects.toMatchObject({ status: 501 });
  });

  it('fences the route, preserves attachment metadata and cancels on disconnect', async () => {
    const handler: unknown = Reflect.get(MessageController.prototype, 'downloadChatMedia');
    expect(Reflect.getMetadata(CHAT_SCOPED_KEY, handler as object)).toBeTruthy();
    const { instance, getMessageMedia } = service();
    const controller = new MessageController(instance, {} as BulkMessageService, {} as ChatScopeService);
    const res = new EventEmitter() as EventEmitter & { set: jest.Mock; removeHeader: jest.Mock };
    res.set = jest.fn();
    res.removeHeader = jest.fn();
    await controller.downloadChatMedia('s', 'c', 'm', '10', res as unknown as Response);
    expect(res.set).toHaveBeenCalledWith('Content-Disposition', "attachment; filename*=UTF-8''r%C3%A9sum%C3%A9.pdf");
    expect(res.listenerCount('close')).toBe(0);
    getMessageMedia.mockImplementationOnce(() => new Promise(() => undefined));
    const pending = controller.downloadChatMedia('s', 'c', 'm', undefined, res as unknown as Response);
    res.emit('close');
    await expect(pending).rejects.toBeInstanceOf(ServiceUnavailableException);
    expect(res.set).toHaveBeenCalledWith('Retry-After', '1');
    expect(res.listenerCount('close')).toBe(0);
  });
});
