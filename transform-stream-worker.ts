import { createReadStream } from "node:fs";
import { Transform, TransformCallback } from "node:stream";
import { parentPort, workerData } from "node:worker_threads";
import { CitiesTemperaturesMapper } from "./cities.js";

const { filePath, start, end, workerNumber } = workerData;

const HIGH_WATER_MARK = 1024 * 1024 * 10; // 10 MB

console.log(
  `Starting worker: ${workerNumber}  - readStream split into ${Math.ceil(
    (end - start) / HIGH_WATER_MARK
  )} chunks.`
);

const readStream = createReadStream(filePath, {
  highWaterMark: HIGH_WATER_MARK,
  encoding: "utf8",
  start,
  end,
});

class SplitNewLineTransformer extends Transform {
  lineOverflow: string = "";
  constructor() {
    super({
      objectMode: false,
      encoding: "utf8",
    });
  }
  _transform(
    chunk: string,
    encoding: BufferEncoding,
    callback: TransformCallback
  ) {
    const lastNewLine = chunk.lastIndexOf("\n");
    if (lastNewLine !== -1) {
      const data = this.lineOverflow + chunk.slice(0, lastNewLine);
      this.lineOverflow = chunk.slice(lastNewLine + 1);
      callback(null, data); // emit data chunk
    } else {
      this.lineOverflow += chunk;
      callback(null); // no chunk emitted
    }
  }
  _flush(callback: TransformCallback) {
    callback(null, this.lineOverflow);
  }
}

const citiesTemperaturesMapper = new CitiesTemperaturesMapper();

readStream
  .pipe(new SplitNewLineTransformer())
  .on("data", (chunk: string) => {
    citiesTemperaturesMapper.processData(chunk);
  })
  .on("end", () => {
    console.log("Ending worker: ", workerNumber);
    parentPort?.postMessage(citiesTemperaturesMapper.cities);
  });
