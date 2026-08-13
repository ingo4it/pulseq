/**
 * Counting semaphore. The worker holds one sized to `WORKER_CONCURRENCY`; a
 * permit is acquired before a job starts and released when it settles, so the
 * number of jobs in flight per worker is hard-bounded. That bound is the
 * consumer half of backpressure — a worker never leases more than it can run.
 */
export class Semaphore {
  private permits: number;
  private readonly waiters: Array<() => void> = [];

  constructor(size: number) {
    this.permits = size;
  }

  get available(): number {
    return this.permits;
  }

  async acquire(): Promise<() => void> {
    if (this.permits > 0) {
      this.permits--;
      return this.releaseOnce();
    }
    await new Promise<void>((resolve) => this.waiters.push(resolve));
    this.permits--;
    return this.releaseOnce();
  }

  private releaseOnce(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.permits++;
      this.waiters.shift()?.();
    };
  }
}
