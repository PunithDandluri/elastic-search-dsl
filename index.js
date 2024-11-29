import express from "express";
import { Client } from "@elastic/elasticsearch";
import multer from "multer";
import csv from "csv-parser";
import fs from "fs";
import { configDotenv } from "dotenv";

const app = express();
configDotenv();
const upload = multer({ dest: "uploads/" });
const esClient = new Client({
  node: process.env.ELASTICSEARCH_URL || "http://localhost:9200",
  auth: {
    username: process.env.ELASTICSEARCH_USERNAME || "elastic",
    password: process.env.ELASTICSEARCH_PASSWORD || "changeme",
  },
});
app.use(express.json());
const mustQueriesBuilder = (req) => {
  const mustQueries = [];
  const {
    Campaign_ID,
    Target_Audience,
    Campaign_Goal,
    Duration,
    Channel_Used,
    Conversion_Rate,
    Acquisition_Cost,
    ROI,
    Location,
    Language,
    Clicks,
    Impressions,
    Engagement_Score,
    Customer_Segment,
    Date,
    Company,
  } = req.body;

  if (Campaign_ID) {
    mustQueries.push({ match: { Campaign_ID } });
  }
  if (Target_Audience) {
    mustQueries.push({ match: { Target_Audience } });
  }
  if (Campaign_Goal) {
    mustQueries.push({ match: { Campaign_Goal } });
  }
  if (Duration) {
    mustQueries.push({ match: { Duration } });
  }
  if (Channel_Used) {
    mustQueries.push({ match: { Channel_Used } });
  }
  if (Conversion_Rate) {
    mustQueries.push({ match: { Conversion_Rate } });
  }
  if (Acquisition_Cost) {
    mustQueries.push({ match: { Acquisition_Cost } });
  }
  if (ROI) {
    mustQueries.push({ match: { ROI } });
  }
  if (Location) {
    mustQueries.push({ match: { Location } });
  }
  if (Language) {
    mustQueries.push({ match: { Language } });
  }
  if (Clicks) {
    mustQueries.push({ match: { Clicks } });
  }
  if (Impressions) {
    mustQueries.push({ match: { Impressions } });
  }
  if (Engagement_Score) {
    mustQueries.push({ match: { Engagement_Score } });
  }
  if (Customer_Segment) {
    mustQueries.push({ match: { Customer_Segment } });
  }
  if (Date) {
    mustQueries.push({ match: { Date } });
  }
  if (Company) {
    mustQueries.push({ match: { Company } });
  }
  return mustQueries;
};
app.listen(3000, () => {
  console.log(
    process.env.ELASTICSEARCH_URL +
      " " +
      process.env.ELASTICSEARCH_USERNAME +
      " " +
      process.env.ELASTICSEARCH_PASSWORD
  );
  console.log("Server is running on port 3000");
});

const BATCH_SIZE = 1000;
app.post("/upload-data", upload.single("file"), async (req, res) => {
  const filePath = req.file.path;

  try {
    const stream = fs.createReadStream(filePath).pipe(csv());
    let bulkData = [];
    let rowCount = 0;

    // Function to send bulk data to Elasticsearch
    const sendBulkData = async (bulkData) => {
      const { body: bulkResponse } = await esClient.bulk({
        refresh: true,
        body: bulkData,
      });

      if (bulkResponse?.errors) {
        const erroredDocuments = bulkResponse.items.filter(
          (item) => item.index && item.index.error
        );
        console.error("Errors in bulk indexing:", erroredDocuments);
      }
    };

    stream.on("data", async (row) => {
      rowCount += 1;
      bulkData.push({ index: { _index: "campaigns" } });
      bulkData.push(row);

      // Check if the batch size is reached
      if (bulkData.length >= BATCH_SIZE * 2) {
        stream.pause(); // Pause the stream temporarily to handle backpressure
        await sendBulkData(bulkData);
        bulkData = []; // Clear bulk data
        stream.resume(); // Resume reading the stream
      }
    });

    stream.on("end", async () => {
      // Upload remaining data if any
      if (bulkData.length > 0) {
        await sendBulkData(bulkData);
      }
      res.status(200).json({
        message: "Data uploaded successfully to Elasticsearch",
        rowsProcessed: rowCount,
      });
      fs.unlinkSync(filePath); // Clean up the uploaded file
    });

    stream.on("error", (error) => {
      res.status(500).json({ message: "Error processing CSV file", error });
      fs.unlinkSync(filePath); // Clean up the uploaded file in case of error
    });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error uploading data to Elasticsearch", error });
    fs.unlinkSync(filePath);
  }
});

app.get("/campaigns", async (req, res) => {
  const mustQueries = mustQueriesBuilder(req);
  console.log(mustQueries);
  const dslQuery =
    mustQueries.length > 0
      ? { query: { bool: { must: mustQueries } } }
      : { query: { match_all: {} } };
  console.log(dslQuery);
  try {
    const searchResponse = await esClient.search({
      index: "campaigns",
      body: dslQuery,
    });
    console.log(searchResponse);
    const campaigns =
      searchResponse?.hits?.hits?.map((hit) => hit?._source) || [];

    res.status(200).json({
      message: "Campaigns retrieved successfully",
      campaigns,
      total: searchResponse?.hits?.total?.value || 0,
    });
  } catch (error) {
    console.error(error);
    return res
      .status(500)
      .json({ message: "Error retrieving campaigns", error });
  }
});

app.get("/", (req, res) => {
  return res.send("Hello World!");
});
