const fastifyPlugin = require("fastify-plugin");

async function alertsRoutes(fastify, options) {
  fastify.get("/fetch-alerts", async (req, reply) => {
    const { fromDate, atmid, dvrip, page = 1, limit = 20 } = req.query;

    if (!fromDate) {
      return reply.status(400).send({ error: "fromDate is required" });
    }

    const client = await fastify.pg.connect();
    try {
      console.log("Connecting to PostgreSQL...");

      // Format the date for the table name (alerts_YYYYMMDD)
      const formattedDate = new Date(fromDate).toISOString().split('T')[0].replace(/-/g, '');  // Format as YYYYMMDD
      const tableName = `alerts_${formattedDate}`;  // Dynamic table name

      console.log("Dynamic table name:", tableName);

      // Pagination calculations
      const offset = (page - 1) * limit;

      // Construct SQL query with dynamic table name, pagination, and filtering
      const sql = `
         SELECT a.customer, a.bank, a.atmid, a.atmshortname, a.siteaddress,
               a.dvrip, a.panel_make, a.City, a.state,
               b.id, b.panelid, b.createtime, b.receivedtime, b.comment,
               b.zone, b.alarm, b.closedby, b.closedtime, b.sendip, a.zone as zon
        FROM sites a
        JOIN ${tableName} b ON (a.oldpanelid = b.panelid OR a.newpanelid = b.panelid)
        WHERE b.receivedtime BETWEEN '${fromDate} 00:00:00' AND '${fromDate} 23:59:59'
        ${atmid ? `AND a.atmid = '${atmid}'` : ''}
        ${dvrip ? `AND a.dvrip = '${dvrip}'` : ''}

      
        ORDER BY b.receivedtime DESC
        LIMIT ${limit} OFFSET ${offset}
      `;

      console.log("Executing SQL query:", sql);
      const { rows } = await client.query(sql);
      
      // Query for total records without pagination
      const totalRecordsQuery = `
        SELECT COUNT(*)  from 
        sites a
        JOIN ${tableName} b ON (a.oldpanelid = b.panelid OR a.newpanelid = b.panelid)
        WHERE b.receivedtime BETWEEN '${fromDate} 00:00:00' AND '${fromDate} 23:59:59'
        ${atmid ? `AND a.atmid = '${atmid}'` : ''}
        ${dvrip ? `AND a.dvrip = '${dvrip}'` : ''}
        
      `;
      console.log("totalRecordsQuery Executing SQL query:", totalRecordsQuery);

      const totalRecordsResult = await client.query(totalRecordsQuery);
      const totalRecords = totalRecordsResult.rows[0].count;

      console.log("Fetched rows:", rows);

      let alerts = [];
      for (const row of rows) {
        let panelTable = getPanelTable(row.Panel_Make);

        let panelQuery = `SELECT sensorname as description, camera FROM ${panelTable} WHERE zone='${row.zone}' AND scode='${row.alarm}'`;
        console.log("Executing panel query:", panelQuery);
        const panelResult = await client.query(panelQuery);
        const panelData = panelResult.rows[0] || { description: "N/A", Camera: "N/A" };

        let alarmMessage = panelData.description;
        if (row.alarm.endsWith("R")) {
          alarmMessage += " Restoral";
        }

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
          incidentDateTime: row.receivedtime,
          alarmReceivedDateTime: row.receivedtime,
          closeDateTime: row.closedtime,
          DVRIP: row.dvrip,
          panelMake: row.panel_make,
          panelID: row.panelid,
          bank: row.bank,
          reactive: row.alarm.endsWith("R") ? "Non-Reactive" : "Reactive",
          closedBy: row.closedby,
          closedDate: row.closedtime,
          remark: `${row.closedtime} * ${row.comment} * ${row.closedby}`,
          sendIp: row.sendip,
          testingByServiceTeam: "N/A",
          testingRemark: "N/A"
        });
      }

      return reply.send({
        itemsPerPage : 20, 
        status: "success",
        totalAlerts: alerts.length,
        totalRecords: totalRecords,
        alerts,
        totalPages: Math.ceil(totalRecords / limit),
        currentPage: parseInt(page),
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
