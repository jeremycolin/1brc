import { CitiesTemperaturesMapper } from "./cities.js";
import { Worker } from "worker_threads";
import { readSync, writeFileSync } from "fs";
import { open } from "node:fs/promises";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

console.time("start");

// const TEST_FILE = "test-files/measurements-4.txt";
// const TEST_FILE = "test-files/measurements-1M.txt";
// const TEST_FILE = "test-files/measurements-100M.txt";
const TEST_FILE = "./1brc/measurements.txt";

const WORKERS_NUMBER = 16;

const FIND_NEWLINE_SIZE = 100;

(async () => {
  const citiesMapper = new CitiesTemperaturesMapper();

  const filePath = TEST_FILE;
  const file = await open(filePath, "r");
  const stat = await file.stat();

  const fileSize = stat.size;
  console.log("file size", fileSize);

  const chunksSize = Math.ceil(fileSize / WORKERS_NUMBER);
  console.log("chunks size", (chunksSize / 1e6).toFixed(2), "MB");

  let activeWorkers = 0;
  let start = 0;
  let end = start + chunksSize - 1;

  for (let i = 0; i < WORKERS_NUMBER; i++) {
    if (i === WORKERS_NUMBER - 1) {
      end = fileSize - 1;
    } else {
      // find a split on newline character
      const dataSlice = Buffer.allocUnsafe(FIND_NEWLINE_SIZE);
      readSync(file.fd, dataSlice, 0, FIND_NEWLINE_SIZE, end);
      end = end + dataSlice.indexOf("\n") - 1; // skip newline character
    }

    const pipeWorker = new Worker(resolve(__dirname, "worker.ts"), {
      workerData: { start, end, filePath, workerNumber: i },
    });
    activeWorkers++;

    pipeWorker.on("message", (workerCities) => {
      citiesMapper.mergeCities(workerCities);
    });

    pipeWorker.on("exit", () => {
      activeWorkers--;
      if (activeWorkers === 0) {
        console.log("number of cities: ", citiesMapper.cities.size);
        console.timeEnd("start");
        writeFileSync("./results-cities.txt", citiesMapper.citiesResults);
      }
    });

    start = end + 2; // skip newline character
    end = end + chunksSize - 1;
  }

  file.close();
})();
