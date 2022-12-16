function showMonthName(){
    const monthNames = ["December","January", "February", "March", "April", "May", "June","July", "August", "September", "October", "November"];
    var d = new Date();    
    document.write(monthNames[d.getMonth()+1]);
}

function showPrevMonthName(){
    const monthNames = ["December","January", "February", "March", "April", "May", "June","July", "August", "September", "October", "November"];
    var d = new Date();    
    document.write(monthNames[d.getMonth()]);
}

function toggleFilters(){
    $('#filter-sorting').toggleClass("open");
    $('.filter-icon').toggleClass("open");
}

function postModalForm(id, form, endpoint, callback) {
	var params = $("#" + form).serialize();
	$.post(endpoint, params, function(data) {
		var response = JSON.parse(data);
		if(response.status === 1) {
			$('#' + id).modal('hide');
			if(typeof callback === 'undefined')
				location.reload();
			else
				callback(response);
		} else {
			// do errors
			if(typeof callback === 'undefined')
				$.each(response.errors, function(k,v) {
					$("#fg-" + k.replace('.','\\.')).addClass("has-error");
					$("#error-" + k.replace('.','\\.')).html(v.toString());
				});
			else
				callback(response);
		}
	});
}

function addElemToFieldSet(field, v) {
	var set = new Set($(field).val().split(','));
	set.add(v);
	$(field).val(Array.from(set).join(','));
}

function removeElemFromFieldSet(field, v) {
	var set = new Set($(field).val().split(','));
	set.delete(v);
	$(field).val(Array.from(set).join(','));
}

function fieldSetHasElem(field, v) {
	return new Set($(field).val().split(',')).has(v);
}