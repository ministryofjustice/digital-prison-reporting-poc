
function change_data_type(col) {
	console.log("Change Data Type " + col);
}

function anonymize(col) {
	console.log("Anonymize " + col);
}


function jexcel_context_menu(el, x, y, e) {
	var items = [];
	
	// if x != null then the custom menu is based on the column
	// get the metadata from the column
	// show the menu
	
	if (y == null) { 
		
		// CHANGE DATA TYPE
		items.push({
			title : "Change data type",
			onclick: function() { change_data_type(x); }
		});

		// ANONYMIZE
		items.push({
			title : "Anonymize",
			onclick: function() { anonymize(x); }
		});
		
		// CUSTOM TRANSFORM (JAVASCRIPT)
		items.push({
			title : "Custom transform",
			onclick: function() { anonymize(x); }
		});
		
		items.push({type: "line"});
		
		// KEEP ROWS IF
		items.push({
			title : "Filter",
			onclick: function() { anonymize(x); }
		});
		
		// REPLACE
		items.push({
			title : "Find/Replace",
			onclick: function() { anonymize(x); }
		});
		
		// DEFAULT NULL
		items.push({
			title : "Default Null Cells",
			onclick: function() { anonymize(x); }
		});
		
		items.push({type: "line"});
		
		// 
		items.push({
			title : "Delete Column",
			onclick: function() { anonymize(x); }
		});
		
	} else {
		
	}
	
	console.log ("x=" + x + ",y=" + y + ",e=" + e);
	
	
	return items;
}
