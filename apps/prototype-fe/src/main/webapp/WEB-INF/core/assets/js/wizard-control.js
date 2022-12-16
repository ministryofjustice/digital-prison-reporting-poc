 $(document).ready(function() {
 	
 	// click the metrics button
 	$('#button-metrics').click(function() {
 		console.log('Metrics Button clicked');
 	});
 	
 	
 	
 	$('.chart-next').click(function()  {
 		console.log("Next");
 		var nextId = $(this).parents('.tab-pane').next().attr("id");
 		if(undefined === nextId) {
 			nextId = $('.tab-pane').first().attr("id");
 		}
		$('.nav-tabs a[href="#'+nextId+'"]').tab('show');
 	});
 	
 	$('.chart-previous').click(function()  {
 		console.log("Previous");
 		var prevId = $(this).parents('.tab-pane').prev().attr("id");
 		if(undefined === prevId) {
 			prevId = $('.tab-pane').last().attr("id");
 		}
		$('.nav-tabs a[href="#'+prevId+'"]').tab('show');
 	});
 	
 	$('#product-table > tbody > tr').each(function() {
	    $(this).css('cursor','pointer').hover(
            function(){ 
                $(this).addClass('active'); 
            },  
            function(){ 
                $(this).removeClass('active'); 
            }).click( function(){ 
                // do something here document.location = $(this).attr('data-href'); 
            }
        );
	});
 });