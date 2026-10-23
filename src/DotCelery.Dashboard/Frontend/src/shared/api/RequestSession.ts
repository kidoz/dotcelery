export class RequestSession {
  private version = 0;
  private controller: AbortController | null = null;
  begin() {
    this.controller?.abort();
    this.controller = new AbortController();
    return { version: ++this.version, signal: this.controller.signal };
  }
  isCurrent(ticket: { version: number }) {
    return ticket.version === this.version;
  }
  invalidate() {
    this.version++;
    this.controller?.abort();
  }
}
