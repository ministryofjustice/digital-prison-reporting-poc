/**
	Widgets for Multi-Select Key Values 
**/


function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

function listToOptions(options) {
	var sel = $('<select>').addClass("selectpicker").attr('tabindex', -98).addClass(id).attr('name',id).attr('data-live-search',true);
	sel.append($("<option>").attr('value','').text(''));
	$(options).each(function() {
		var k = this.split(":");
		sel.append($("<option>").attr('value',k[1]).text(k[0]));
	});
	return sel;
}

function objectToOptions(options, id) {
	var sel = $('<select>').addClass("selectpicker").attr('tabindex', -98).addClass(id).attr('name',id).attr('data-live-search',true);
	Object.keys(options).forEach(function(key) {
	  console.table('Key : ' + key + ', Value : ' + options[key])
	  sel.append($("<option>").attr('value',options[key]).text(key));
	})
	return sel;
}


function addMultiSelectKeyValueRow(container, list, id) {

	var uuid = uuidv4();
	
	// turn the list into options
	var options = objectToOptions(list, id + "_attr");

	var newRow = $('<div>').addClass("col-lg-12").addClass("form-inline").attr('id', uuid);
	var sc = $('<div>').addClass("col-lg-4");
	sc.append(options);
	newRow.append(sc);
	$(options).selectpicker();
	var ic = $('<div>').addClass("col-lg-8");
	var dbtn = $('<button>').addClass('floating-delete').addClass(id + '_delete');
	dbtn.append('<i class="ion-trash-a text-danger"></i>');
	$(dbtn).click(function(event) {  console.log("delete " + uuid); deleteMSelectRow(uuid); return false; });
	ic.append(dbtn);
    ic.append('<input type="number" step="0.01" name="' + id + '_value" placeholder="" class="form-control input-lg ' + id + '_value" value="">')
	newRow.append(ic);     
	container.append(newRow)
}

function populateMultiSelectKeyValueRows(container, list, id, values) {
	Object.keys(values).forEach(function(key) {
	  console.table('Key : ' + key + ', Value : ' + values[key])
	  addMultiSelectKeyValueRow(container, list, id);
	  $(container).find('.' + id + '_attr').last().val(key);
	  $(container).find('.' + id + '_value').last().val(values[key]);
	})
}

function convertMultiSelectIntoString(id) {
	// get all the values in the collection
	var attrs = $("." + id + '_attr');
	var vals =  $("." + id + '_value');
	var result = "";
	for (i = 0; i < attrs.length; i++) {
		var attr = $(attrs[i]).find(":selected").val();
		var val = $(vals[i]).val();
		result = result + attr + ":" + val+ ",";
	}
	result = result.substring(0, result.length-1);
	return result;
}


