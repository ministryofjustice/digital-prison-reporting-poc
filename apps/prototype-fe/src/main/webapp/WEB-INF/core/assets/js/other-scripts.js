
	
	// ==================================
    // Add Domain to Product
    // POST /product/{id}/domain/{dom.id}
    // ==================================
    
    /*
    function addDomainToProduct(d, name) {
    	var endpoint = '/product/${values['id']!}/domain/' + d;
		$.post(endpoint, function(data) {
			var response = JSON.parse(data);
			if(response.status === 1) {
				// grab the template
				var template = $('.domain-template').clone().removeClass('domain-template');
				$(template).attr('id',d);
				// $(template).find('.domain-template-description').html(
				$(template).find('.domain-template-name').html(name);
				$(template).find('.close').on('click', function() {
			    	removeDomainFromProduct($(this).parent().attr('id'), this);
			    });
			    
				template.appendTo('#product-domains');
			} else {
				// do errors
				$.each(response.errors, function(k,v) {
					console.log(v);
				});
			}
		});
    }
	    
	    
	    
    // ==================================
    // Remove Domain from Product
    // DELETE /product/{id}/domain/{dom.id}
    // ==================================
    function removeDomainFromProduct(d, elem) {
    	var endpoint = '/product/${values['id']!}/domain/' + d;
		$.ajax({
			url: endpoint, 
			method: "DELETE",
		})
		.done(function(data) {
			var response = JSON.parse(data);
			if(response.status === 1) {
				$(elem).parent().remove();
			} else {
				// do errors
				$.each(response.errors, function(k,v) {
					console.log(v);
				});
			}
		});
    }
    */
