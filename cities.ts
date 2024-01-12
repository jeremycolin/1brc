export type City = {
  city: string;
  min: number;
  avg: number;
  max: number;
  count: number;
};

export type CityMap = Map<string, City>;

export class CitiesTemperaturesMapper {
  private citiesTemperatures: CityMap = new Map();

  processData(data: string) {
    for (const line of data.split("\n")) {
      const [city, temperatureString] = line.split(";");
      const temperature = +temperatureString;

      const cityTemperatures = this.citiesTemperatures.get(city) ?? {
        city: city,
        min: Infinity,
        max: 0,
        avg: 0,
        count: 0,
      };

      cityTemperatures.min = Math.min(cityTemperatures.min, temperature);
      cityTemperatures.max = Math.max(cityTemperatures.max, temperature);
      cityTemperatures.count++;
      cityTemperatures.avg =
        (cityTemperatures.avg * (cityTemperatures.count - 1) + temperature) /
        cityTemperatures.count;

      this.citiesTemperatures.set(city, cityTemperatures);
    }
  }

  mergeCities(workerCities: CityMap) {
    workerCities.forEach((city) => {
      const cityResults = this.citiesTemperatures.get(city.city) ?? {
        city: city.city,
        min: Infinity,
        avg: 0,
        max: 0,
        count: 0,
      };
      cityResults.min = Math.min(cityResults.min, city.min);
      cityResults.max = Math.max(cityResults.max, city.max);
      cityResults.avg =
        (cityResults.avg * cityResults.count + city.avg * city.count) /
        (cityResults.count + city.count);
      cityResults.count += city.count;

      this.citiesTemperatures.set(city.city, cityResults);
    });
  }

  get cities() {
    return this.citiesTemperatures;
  }

  get citiesResults() {
    //Albuquerque=-38.3/14.0/70.1
    return Array.from(this.citiesTemperatures.values())
      .sort((a, b) => {
        if (a.city < b.city) {
          return -1;
        }
        if (b.city < a.city) {
          return 1;
        }
        return 0;
      })
      .map((city) => {
        return `${city.city}=${city.min.toFixed(1)}/${city.avg.toFixed(
          1
        )}/${city.max.toFixed(1)}`;
      })
      .join("\n");
  }
}
