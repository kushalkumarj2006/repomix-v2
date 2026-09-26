/* ============================================================================
 * Repomix — streaming ZIP writer (constant memory)
 *
 * Builds a ZIP archive chunk-by-chunk using CompressionStream('deflate-raw').
 * The browser's download manager writes each chunk to disk as it arrives, so
 * peak RAM stays at one file plus the DEFLATE window regardless of archive
 * size. A 500 MB repo downloads without ever holding more than a few MB.
 * ========================================================================== */
(function (global) {
  'use strict';

  const CRC_TABLE = (() => {
    const table = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) {
        c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
      }
      table[n] = c >>> 0;
    }
    return table;
  })();

  function crc32(bytes) {
    let c = 0xFFFFFFFF;
    for (let i = 0; i < bytes.length; i++) {
      c = CRC_TABLE[(c ^ bytes[i]) & 0xFF] ^ (c >>> 8);
    }
    return (c ^ 0xFFFFFFFF) >>> 0;
  }

  function u16(n) { return new Uint8Array([n & 0xFF, (n >>> 8) & 0xFF]); }
  function u32(n) {
    return new Uint8Array([n & 0xFF, (n >>> 8) & 0xFF, (n >>> 16) & 0xFF, (n >>> 24) & 0xFF]);
  }
  function concat(chunks) {
    let len = 0;
    for (const c of chunks) len += c.length;
    const out = new Uint8Array(len);
    let off = 0;
    for (const c of chunks) { out.set(c, off); off += c.length; }
    return out;
  }

  const enc = new TextEncoder();
  function toBytes(v) {
    if (v == null) return new Uint8Array(0);
    if (v instanceof Uint8Array) return v;
    if (typeof v === 'string') return enc.encode(v);
    return enc.encode(String(v));
  }

  async function deflateRaw(bytes) {
    const cs = new CompressionStream('deflate-raw');
    const writer = cs.writable.getWriter();
    const writePromise = writer.write(bytes).then(() => writer.close());
    const reader = cs.readable.getReader();
    const chunks = [];
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
    await writePromise;
    return concat(chunks);
  }

  function createStreamingZip(opts) {
    const onProgress = (opts && opts.onProgress) || null;
    const outChunks = [];
    let outSize = 0;
    const central = [];

    function push(bytes) { outChunks.push(bytes); outSize += bytes.length; }

    async function addFile(path, data) {
      const nameBytes = enc.encode(path);
      const nameLen = nameBytes.length;
      const dataBytes = toBytes(data);
      const uncompSize = dataBytes.length;
      const crc = crc32(dataBytes);

      let compBytes;
      if (uncompSize === 0) {
        compBytes = new Uint8Array(0);
      } else {
        compBytes = await deflateRaw(dataBytes);
      }
      const compSize = compBytes.length;
      const offset = outSize;

      push(concat([
        u32(0x04034b50),
        u16(20),
        u16(0x0808),
        u16(8),
        u16(0), u16(0),
        u32(0), u32(0), u32(0),
        u16(nameLen),
        u16(0),
        nameBytes
      ]));
      if (compSize > 0) push(compBytes);
      push(concat([u32(0x08074b50), u32(crc), u32(compSize), u32(uncompSize)]));

      central.push({ name: nameBytes, crc, compSize, uncompSize, offset });
      if (onProgress) onProgress(-1);
    }

    async function finish() {
      const cdStart = outSize;
      for (const e of central) {
        push(concat([
          u32(0x02014b50),
          u16(20), u16(20),
          u16(0x0808),
          u16(8),
          u16(0), u16(0),
          u32(e.crc),
          u32(e.compSize),
          u32(e.uncompSize),
          u16(e.name.length),
          u16(0), u16(0),
          u16(0), u16(0),
          u32(0),
          u32(e.offset),
          e.name
        ]));
      }
      const cdSize = outSize - cdStart;
      const n = central.length;
      push(concat([
        u32(0x06054b50),
        u16(0), u16(0),
        u16(n), u16(n),
        u32(cdSize),
        u32(cdStart),
        u16(0)
      ]));
      return new Blob(outChunks, { type: 'application/zip' });
    }

    return { addFile, finish };
  }

  global.RepomixZip = Object.freeze({ createStreamingZip });
})(self);
