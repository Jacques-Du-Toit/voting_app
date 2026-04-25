const resultsTable = document.getElementById("results_table");
const tableBody = document.getElementById("results_table_body");

export const checkMessageResults = function(serverMessage) {
    if (serverMessage.message_type == "NewOption") {
        const optionStats = serverMessage.content.split(",");
        const tableRow = document.createElement("tr");

        for (const val of optionStats) {
            const tableCell = document.createElement("td");
            
            if (!isNaN(val) && val.trim() !== "") {
                tableCell.textContent = parseFloat(val).toFixed(2);
            } else {
                tableCell.textContent = val;
            }

            tableRow.appendChild(tableCell);
        }
        if (tableBody) {
            tableBody.appendChild(tableRow);
        } else {
            console.error("Could not find the results table body in the HTML");
        }
    } 
}