import { createReadStream } from "fs";
import { parentPort, workerData } from "worker_threads";
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

const citiesTemperaturesMapper = new CitiesTemperaturesMapper();

let lineOverflow = "";

readStream.on("data", (chunk: string) => {
  const lastNewLine = chunk.lastIndexOf("\n");
  if (lastNewLine !== -1) {
    const data = lineOverflow + chunk.slice(0, lastNewLine);
    lineOverflow = chunk.slice(lastNewLine + 1);
    citiesTemperaturesMapper.processData(data);
  } else {
    lineOverflow += chunk;
  }
});

readStream.on("end", () => {
  console.log("Ending worker: ", workerNumber);
  if (lineOverflow) {
    citiesTemperaturesMapper.processData(lineOverflow);
  }
  parentPort?.postMessage(citiesTemperaturesMapper.cities);
});
