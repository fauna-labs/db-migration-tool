const { Client, fql } = require("fauna");
const { InvalidArgumentError } = require("commander");

async function validate(options) {
  function validateField(sourceRow, targetRow, key) {
    if (key !== "ts") {
      if (typeof sourceRow[key] === "object") {
        for (let subkey in sourceRow[key]) {
          validateField(sourceRow[key], targetRow[key], subkey);
        }
      } else {
        if (sourceRow[key] !== targetRow[key]) {
          throw new Error(`Field '${key}': ${sourceRow[key]} <> ${targetRow[key]}`);
        }
      }
    }
  }

  const pageSize = Math.min(options.validate, 1000);
  const sourceClient = new Client({ secret: options.source });
  const targetClient = new Client({ secret: options.target });
  const query = fql`${fql([options.collection])}.all().paginate(${pageSize})`;

  let sourceData, targetData;

  do {
    if (sourceData && sourceData.data.after) {
      sourceData = await sourceClient.query(fql`Set.paginate(${sourceData.data.after})`);
      targetData = await targetClient.query(fql`Set.paginate(${targetData.data.after})`);
    } else {
      sourceData = await sourceClient.query(query);
      targetData = await targetClient.query(query);
    }

    if (sourceData.data.data.length !== targetData.data.data.length) {
      throw new Error("MISMATCH in count of documents in the current page; can't continue");
    }

    for (let i = 0; i < sourceData.data.data.length; i++) {
      let sourceRow = sourceData.data.data[i];
      let targetRow = targetData.data.data[i];

      for (let key in sourceRow) {
        try {
          validateField(sourceRow, targetRow, key);
        } catch (error) {
          console.error(`Mismatch found in doc ID '${sourceRow["id"]}'`);
          throw error;
        }
      }
    }

    if (sourceData.data.after) {
      await pause(10000).then(
        console.log(`Waiting 10s and continuing with page token ${sourceData.data.after}`)
      );
    }
  } while (sourceData.data.after)
}

async function pause(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseParallelism(value, dummyPrevious) {
  let parsedValue = parseInt(value, 10);

  if (isNaN(parsedValue)) {
    throw new InvalidArgumentError("Invalid 'parallelism' arg; please pass an integer from 1 to 10");
  }

  if (parsedValue > 10) {
    console.warn("Parallelism set to a max of 10");
    parsedValue = 10;
  } else if (parsedValue < 1) {
    console.warn("Parallelism set to a min of 1");
    parsedValue = 1;
  }

  return parsedValue;
}

module.exports = { pause, parseParallelism, validate };
