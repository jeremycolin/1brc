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
  let start = 0;
  let end = chunk.indexOf("\n");

  while (end !== -1) {
    citiesTemperaturesMapper.processDataFaster(
      lineOverflow + chunk.slice(start, end)
    );

    lineOverflow = "";

    start = end + 1;
    end = chunk.indexOf("\n", start);
  }
  lineOverflow = chunk.slice(start);
});

readStream.on("end", () => {
  console.log("Ending worker: ", workerNumber);
  if (lineOverflow) {
    citiesTemperaturesMapper.processData(lineOverflow);
  }
  parentPort?.postMessage(citiesTemperaturesMapper.cities);
});
