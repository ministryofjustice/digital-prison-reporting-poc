[#ftl]
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en" xml:lang="en">
	<head>
		<title>Register with Modular Data</title>
		<style>
			body {
				background-color: #FFFFFF !important;
			}
		</style>
		
		[#--  Stripe --]
		<script src="https://js.stripe.com/v3/"></script>
		
		
		<script>
			[#--  Stripe
			var key = ${key!};
			var setupIntent = ${intent!};
			
			function setupStripe() {
			  	var stripe = Stripe(key);
			  	var elements = stripe.elements();
			
			  	// Element styles
			  	var style = {
			    	base: {
			      	fontSize: "16px",
			      	color: "#32325d",
			      	fontFamily:
			        	"-apple-system, BlinkMacSystemFont, Segoe UI, Roboto, sans-serif",
			      	fontSmoothing: "antialiased",
			      	"::placeholder": {
			        	color: "rgba(0,0,0,0.4)"
			      	}
			    	}
			  	};
			
			  	var card = elements.create("card", { style: style });
			
			  	card.mount("#card-element");
			
			  	// Element focus ring
			  	card.on("focus", function() {
			    	var el = document.getElementById("card-element");
			    	el.classList.add("focused");
			  	});
			
			  	card.on("blur", function() {
			    	var el = document.getElementById("card-element");
			    	el.classList.remove("focused");
			  	});
			
			  	// Handle payment submission when user clicks the pay button.
			  	
			  	$('#signupform').submit(function(e) {
			  		
			  		
			  		// if intent is filled in then let's submit;
			  		if($('#intent').val().length > 0) {
			  			$('#signupform').unbind(e);
			  			$('#signupform').submit();
					    return true;
					}
					
					e.preventDefault();
			  		
			  		stripe.confirmCardSetup(setupIntent.client_secret, {
			        	payment_method: {
			          		card: card,
			          		billing_details: { email: $('#username').val() }
			        	}	
			      	})
			      	.then(function(result) {
			        	if (result.error) {
			          		var displayError = document.getElementById("card-errors");
			          		displayError.textContent = result.error.message;
			        	} else {
			          		console.log("successful card setup");
			          		$('#intent').val(result.setupIntent.id);
			  				$('#signupform').unbind(e);
					        $('#signupform').submit();
			        }
			  	});
			  	
			  });
			};
			
			$(document).ready(function() {
				setupStripe(); 
			});
			
			--]
			
			(function ($) {
			    $.toggleShowPassword = function (options) {
			        var settings = $.extend({
			            field: "#password",
			            control: "#toggle_show_password",
			        }, options);
			
			        var control = $(settings.control);
			        var field = $(settings.field)
			
			        control.bind('click', function () {
			            if (control.is(':checked')) {
			                field.attr('type', 'text');
			            } else {
			                field.attr('type', 'password');
			            }
			        })
			    };
			}(jQuery));
		</script>
	</head>
	<body>
	
		[#assign hasUsernameError = (errors!?size > 0 && errors['username']!?length > 0)/]
		[#assign hasPasswordError = (errors!?size > 0 && errors['password']!?length > 0)/]
		
		
		<div class="container">
	        <div class="row">
	            <div class="col-md-5 col-md-offset-4">
	                <div class="login-panel panel panel-default">
	                    <div class="panel-body">
	                    	<h2 style="text-align:center">Register for free with<br/><strong>Modular Data</strong></h2>
	                    	<h4 style="text-align:center;color:#b7c4cd">It's simple and takes less than a minute</h4>
	                        <form id="signupform" role="form" action="/signup" method="post">
	                        	<input class="hidden" type="hidden" id="accountRef" name="accountRef" value="${accountReference!}" />
	                        	<input class="hidden" type="hidden" id="intent" name="intent"/>
	                            <fieldset>
	                                <div class="form-group[#if hasUsernameError] has-error[/#if]">
	                                	[#if hasUsernameError]<label class="control-label" for="username">${errors['username']}</label>[/#if]
	                                    <input class="form-control" placeholder="Your email" id="username" name="username" type="email" value="${username!}" autofocus>
	                                </div>
	                                <div class="form-group[#if hasPasswordError] has-error[/#if]">
	                                	[#if hasPasswordError]<label class="control-label" for="password">${errors['password']}</label>[/#if]
	                                    <input class="form-control" placeholder="Your password (8 or more characters)" id="password" name="password" type="password" value="${password!}" autocomplete="off">
	                                </div>
	                                <div class="form-group">
	                                    <input class="form-control" placeholder="Your full name" id="name" name="name" value="${name!}" autocomplete="off">
	                                </div>
	                                <div class="form-group">
	                                    <input class="form-control" placeholder="Organisation name" id="organization" name="organization" value="${organization!}" autocomplete="off">
	                                </div>
	                                <hr/>
	                                <div class="form-group">
	                                    <input class="form-control" placeholder="Optional - your phone (complete with country code)" id="phone" name="phone" value="${phone!}" autocomplete="off">
	                                </div>
	                                [#-- Stripe 
	                                <div class="form-group">
	                                	<label class="control-label" for="card">Payment Details</label>
	                                    <div id="card-element"></div>
	                                </div>
	                                <div class="form-group">
	                                	<div id="card-errors" role="alert"></div>
	                                </div>
	                                --]
		                            <button id="submit" type="submit" class="btn btn-lg btn-success btn-block">
		                            	<div class="spinner hidden" id="spinner"></div>
            							<span id="button-text">Create My Account</span>
            						</button>
	                                <div class="foot">
	                                	<br/>
	                                	<p class="text-center"><a href="/authenticate"><small>Already registered?</small></a></p>
	                                	<p class="text-center"><small style="color:#b7c4cd">By signing up, you agree to our <a href="http://modulardata.io/terms.html" target="_blank">Terms of Service</a> and <a href="http://modulardata.io/privacy-policy.html" target="_blank">Privacy Policy</a>,<br/>including use of cookies.</small></p>
		                            </div>
	                            </fieldset>
	                        </form>
	                    </div>
	                    [#-- 
	                    [#include "/social/social.ftl"/]
	                     --]
	                </div>
	            </div>
	        </div>
	    </div>
		
	
    </body>
</html>