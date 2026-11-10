import { ServiceUnavailableException } from '@nestjs/common';
import { inboundMediaTimeoutMs } from '../../engine/adapters/inbound-media-cap';

/** Bound lookup, queue and download together. Page downloads cannot be cancelled once started;
 * callers must keep their concurrency slot until the underlying promise settles. */
export async function onDemandMediaRead<T>(
  read: (signal: AbortSignal) => Promise<T>,
  parent?: AbortSignal,
): Promise<T> {
  const controller = new AbortController();
  const abort = () => controller.abort();
  parent?.addEventListener('abort', abort, { once: true });
  if (parent?.aborted) abort();
  const timer = setTimeout(abort, inboundMediaTimeoutMs());
  let rejectWait: () => void = () => undefined;
  const stopped = new Promise<never>((_, reject) => {
    rejectWait = () => reject(new ServiceUnavailableException('Media read interrupted or timed out; retry shortly'));
    controller.signal.addEventListener('abort', rejectWait, { once: true });
    if (controller.signal.aborted) rejectWait();
  });
  try {
    return await Promise.race([
      stopped,
      Promise.resolve().then(() => {
        controller.signal.throwIfAborted();
        return read(controller.signal);
      }),
    ]);
  } finally {
    clearTimeout(timer);
    parent?.removeEventListener('abort', abort);
    controller.signal.removeEventListener('abort', rejectWait);
  }
}
