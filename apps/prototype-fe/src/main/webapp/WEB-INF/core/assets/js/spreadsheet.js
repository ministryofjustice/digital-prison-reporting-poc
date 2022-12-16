/**
	Spreadsheet Javascript
**/

function spreadsheet_onselection(instance, x1, y1, x2, y2, origin) {
    var cellName1 = jexcel.getColumnNameFromId([x1, y1]);
    var cellName2 = jexcel.getColumnNameFromId([x2, y2]);
    var header = $(instance).jexcel('getHeader', x1);
    console.log('The selection from ' + cellName1 + ' to ' + cellName2 + ' Header: ' + header );
    // we must preview the data in the selection area based on the column data
    
}
