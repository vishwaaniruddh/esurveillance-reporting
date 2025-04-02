const fastifyPlugin = require("fastify-plugin");

async function alertsRoutes(fastify, options) {
  fastify.get("/fetch-alerts", async (req, reply) => {
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
      console.log("Tracker result:", trackerResult.rows);

      if (trackerResult.rowCount === 0) {
        console.log("No new alerts to process.");
        return reply.send({ message: "No new alerts to process." });
      }
      const { id: recordId, alerts_data_date, newtablename } = trackerResult.rows[0]; 
      const newtableName = newtablename; // Fix assignment
      
      const reportDate = new Date(alerts_data_date).toISOString().split('T')[0];

      console.log("✅ Using formatted alerts_data_date:", reportDate);
      
      console.log("Found alerts data:", { recordId, alerts_data_date, newtableName });

      // Define SQL query
      const sql = `
        SELECT a.Customer, a.Bank, a.ATMID, a.ATMShortName, a.SiteAddress,
               a.DVRIP, a.Panel_Make, a.City, a.State,
               b.id, b.panelid, b.createtime, b.receivedtime, b.comment,
               b.zone, b.alarm, b.closedBy, b.closedtime, b.sendip,a.Zone as zon
        FROM sites a
        JOIN ${newtableName} b ON (a.OldPanelID = b.panelid OR a.NewPanelID = b.panelid)
        WHERE b.receivedtime BETWEEN '${reportDate} 00:00:00' AND '${reportDate} 23:59:59'
        ORDER BY b.receivedtime DESC
        LIMIT 1000
      `;




      console.log("Executing SQL query:", sql);
      const { rows } = await client.query(sql);
      console.log("Fetched rows:", rows);

      let alerts = [];
      for (const row of rows) {
        console.log("Processing row:", row);

        let panelTable = getPanelTable(row.Panel_Make);
        let id = getPanelTable(row.id);


        let panelQuery = `SELECT SensorName as Description, Camera FROM ${panelTable} WHERE ZONE='${row.zone}' AND SCODE='${row.alarm}'`;
        console.log("Executing panel query:", panelQuery);
        const panelResult = await client.query(panelQuery);
        const panelData = panelResult.rows[0] || { Description: "N/A", Camera: "N/A" };

        let alarmMessage = panelData.Description;
        if (row.alarm.endsWith("R")) {
          alarmMessage += " Restoral";
        }

        alerts.push({
          clientName: row.Customer,
          incidentNumber: row.id,
          region: row.zon,
          ATMID: row.ATMID,
          address: row.SiteAddress,
          city: row.City,
          state: row.State,
          zone: row.Zone,
          alarm: row.alarm,
          incidentCategory: panelData.Description,
          alarmMessage: alarmMessage,
          incidentDateTime: row.receivedtime,
          alarmReceivedDateTime: row.receivedtime,
          closeDateTime: row.closedtime,
          DVRIP: row.DVRIP,
          panelMake: row.Panel_Make,
          panelID: row.panelid,
          bank: row.Bank,
          reactive: row.alarm.endsWith("R") ? "Non-Reactive" : "Reactive",
          closedBy: row.closedBy,
          closedDate: row.closedtime,
          remark: `${row.closedtime} * ${row.comment} * ${row.closedBy}`,
          sendIp: row.sendip,
          testingByServiceTeam: "N/A",
          testingRemark: "N/A"
        });
      }

      // Mark the report as created
      console.log("Marking report as created...");
      await client.query(`UPDATE alerts_data_tracker_datewise SET is_viewalertReportCreated = 1 WHERE id = $1`, [recordId]);

      return reply.send({
        status: "success",
        reportDate,
        totalAlerts: alerts.length,
        alerts
      });
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
      "comfort_diebold": "comfort_diebold"
    };
    return panelTables[panelMake] || "rass";
  }
}

module.exports = fastifyPlugin(alertsRoutes);
