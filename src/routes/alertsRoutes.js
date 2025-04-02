const fastifyPlugin = require("fastify-plugin");
const { Parser } = require("json2csv");
const fs = require("fs");
const archiver = require("archiver");
const path = require("path");

async function exportAlertsRoutes(fastify, options) {
  
  fastify.get("/export-alerts", async (req, reply) => {
    const client = await fastify.pg.connect();
    try {
      console.log("Connecting to PostgreSQL...");

      // Get latest unprocessed alert entry
      const trackerQuery = `
        SELECT id, alerts_data_date, newtableName 
        FROM alerts_data_tracker_datewise
        WHERE is_viewalertReportCreated = '0' 
        ORDER BY id DESC LIMIT 1;
      `;
      console.log("Executing trackerQuery:", trackerQuery);
      const trackerResult = await client.query(trackerQuery);
      
      if (trackerResult.rowCount === 0) {
        console.log("No new alerts to process.");
        return reply.send({ message: "No new alerts to process." });
      }
      
      const { id: recordId, alerts_data_date, newtablename } = trackerResult.rows[0]; 
      const reportDate = new Date(alerts_data_date).toISOString().split("T")[0];

      console.log("✅ Using formatted alerts_data_date:", reportDate);

      // Define SQL query to fetch data from the tables
      const sql = `
        SELECT a.customer, a.bank, a.atmid, a.atmshortname, a.siteaddress,
               a.dvrip, a.panel_make, a.City, a.state,
               b.id, b.panelid, b.createtime, b.receivedtime, b.comment,
               b.zone, b.alarm, b.closedby, b.closedtime, b.sendip, a.zone as zon
        FROM sites a
        JOIN ${newtablename} b ON (a.oldpanelid = b.panelid OR a.newpanelid = b.panelid)
        WHERE b.receivedtime BETWEEN '${reportDate} 00:00:00' AND '${reportDate} 23:59:59'
      
        UNION ALL
      
        SELECT a.customer, a.bank, a.atmid, a.atmshortname, a.siteaddress,
               a.dvrip, a.panel_make, a.City, a.state,
               b.id, b.panelid, b.createtime, b.receivedtime, b.comment,
               b.zone, b.alarm, b.closedby, b.closedtime, b.sendip, a.zone as zon
        FROM sites a
        JOIN back${newtablename} b ON (a.oldpanelid = b.panelid OR a.newpanelid = b.panelid)
        WHERE b.receivedtime BETWEEN '${reportDate} 00:00:00' AND '${reportDate} 23:59:59'
      
        ORDER BY receivedtime DESC
      `;
      
      console.log("Executing SQL query:", sql);
      const { rows } = await client.query(sql);

      // Define maximum rows per CSV (800,000)
      const MAX_RECORDS_PER_CSV = 800000;
      let chunkedData = [];

      // Split rows into chunks based on MAX_RECORDS_PER_CSV
      for (let i = 0; i < rows.length; i += MAX_RECORDS_PER_CSV) {
        chunkedData.push(rows.slice(i, i + MAX_RECORDS_PER_CSV));
      }

      // Initialize array to store file paths
      const filePaths = [];
      
      // For each chunk of data, create a separate CSV file
      for (let i = 0; i < chunkedData.length; i++) {
        const chunk = chunkedData[i];

        // Process the chunk
        let alerts = [];
        for (const row of chunk) {
          let panelTable = getPanelTable(row.Panel_Make);

          let panelQuery = `SELECT sensorname as description, camera FROM ${panelTable} WHERE zone='${row.zone}' AND scode='${row.alarm}'`;
          console.log("Executing panel query:", panelQuery);
          const panelResult = await client.query(panelQuery);
          const panelData = panelResult.rows[0] || { description: "N/A", Camera: "N/A" };

          let alarmMessage = panelData.description;
          if (row.alarm.endsWith("R")) {
            alarmMessage += " Restoral";
          }

          // Convert incidentDateTime to the format Y-m-d H:i:s
          const incidentDateTimeFormatted = formatDateTime(row.receivedtime);

          alerts.push({
            clientName: row.customer,
            incidentNumber: row.id,
            region: row.zon,
            ATMID: row.atmid,
            address: row.siteaddress,
            city: row.city,
            state: row.state,
            zone: row.zone,
            alarm: row.alarm,
            incidentCategory: panelData.description,
            alarmMessage: alarmMessage,
            incidentDateTime: incidentDateTimeFormatted, // Use formatted incidentDateTime
            alarmReceivedDateTime: incidentDateTimeFormatted, // Use formatted alarmReceivedDateTime
            closeDateTime: row.closedtime ? formatDateTime(row.closedtime) : null, // Optional field
            DVRIP: row.dvrip,
            panelMake: row.panel_make,
            panelID: row.panelid,
            bank: row.bank,
            reactive: row.alarm.endsWith("R") ? "Non-Reactive" : "Reactive",
            closedBy: row.closedby,
            closedDate: row.closedtime ? formatDateTime(row.closedtime) : null, // Optional field
            remark: `${row.closedtime ? formatDateTime(row.closedtime) : ''} * ${row.comment} * ${row.closedby}`,
            sendIp: row.sendip,
            testingByServiceTeam: "N/A",
            testingRemark: "N/A",
          });
        }

        // Convert JSON to CSV for this chunk
        const json2csvParser = new Parser();
        const csv = json2csvParser.parse(alerts);

        // Write CSV to a file
        const filePath = path.join(__dirname, 'reports', `alerts_${reportDate}_part_${i + 1}.csv`);
        fs.writeFileSync(filePath, csv);
        filePaths.push(filePath);
      }

      // Create a zip file containing all CSV files
      const zipFilePath = path.join(__dirname, 'reports', `alerts_${reportDate}.zip`);
      const output = fs.createWriteStream(zipFilePath);
      const archive = archiver('zip', { zlib: { level: 9 } });

      archive.pipe(output);
      filePaths.forEach((filePath) => {
        archive.file(filePath, { name: path.basename(filePath) });
      });

      archive.finalize();

      // After zipping, send the response with the zip file
      output.on('close', () => {
        console.log(`Zipped CSVs successfully created: ${zipFilePath}`);
        // reply.sendFile(`alerts_${reportDate}.zip`);  // Send the zip file as response
      });

      // Mark the report as created
      console.log("Marking report as created...");
      await client.query(`UPDATE alerts_data_tracker_datewise SET is_viewalertReportCreated = 1 WHERE id = $1`, [recordId]);

    } catch (err) {
      console.error("Error occurred:", err);
      reply.status(500).send({ error: "Internal Server Error", message: err.message });
    } finally {
      client.release();
    }
  });

  // Function to get panel-specific table
  function getPanelTable(panelMake) {
    const panelTables = {
      "SMART -I": "smarti",
      "SMART-IN": "smartinew",
      "SEC": "securico",
      "sec_sbi": "sec_sbi",
      "RASS": "rass",
      "rass_cloud": "rass_cloud",
      "rass_sbi": "rass_sbi",
      "Raxx": "raxx",
      "securico_gx4816": "securico_gx4816",
      "smarti_hdfc32": "smarti_hdfc32",
      "comfort_diebold": "comfort_diebold",
    };
    return panelTables[panelMake] || "rass";
  }

  // Function to format the date into Y-m-d H:i:s
  function formatDateTime(dateTime) {
    const date = new Date(dateTime);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0'); // Get month and pad with 0
    const day = String(date.getDate()).padStart(2, '0'); // Get day and pad with 0
    const hours = String(date.getHours()).padStart(2, '0'); // Get hours and pad with 0
    const minutes = String(date.getMinutes()).padStart(2, '0'); // Get minutes and pad with 0
    const seconds = String(date.getSeconds()).padStart(2, '0'); // Get seconds and pad with 0
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
  }
}

module.exports = fastifyPlugin(exportAlertsRoutes);
