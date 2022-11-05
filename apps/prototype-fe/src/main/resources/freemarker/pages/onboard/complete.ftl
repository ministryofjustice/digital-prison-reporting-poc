[#ftl]
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en" xml:lang="en">
	<head>
		<title>Complete Registration with Modular Foundry</title>
		<style>
			body {
				background-color: #FFFFFF !important;
			}
		</style>
		<script>
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
	                    	<h2 style="text-align:center">Complete Registration for your<br/><strong>Modular Data account</strong></h2>
	                    	<h4 style="text-align:center;color:#b7c4cd">Almost done! One last thing we need.</h4>
	                        <form role="form" action="/complete" method="post" onsubmit="javascript: return valid.check(this);">
	                            <fieldset>
	                                <div class="form-group[#if hasUsernameError] has-error[/#if]">
	                                	[#if hasUsernameError]<label class="control-label" for="username">${errors['username']}</label>[/#if]
	                                    <input class="form-control" placeholder="Your email" id="username" name="username" type="email" value="${username!}" autofocus>
	                                </div>
	                                <div class="form-group">
	                                    <input class="form-control" placeholder="Your full name" id="name" name="name" value="${name!}" autocomplete="off">
	                                </div>
	                                <div class="form-group">
	                                    <input class="form-control" placeholder="Organization name" id="organization" name="organization" value="${organization!}" autocomplete="off">
	                                </div>
	                                <hr/>
	                                <div class="form-group">
	                                    <input class="form-control" placeholder="Optional - your phone (complete with country code)" id="phone" name="phone" value="${phone!}" autocomplete="off">
	                                </div>
		                            <button type="submit" class="btn btn-lg btn-success btn-block">Create My Account</button>
	                                <div class="foot">
	                                	<br/>
	                                	<p class="text-center"><a href="/authenticate"><small>Already registered?</small></a></p>
	                                	<p class="text-center"><small style="color:#b7c4cd">By signing up, you agree to our <a href="http://modulardata.io/terms.html" target="_blank">Terms of Service</a> and <a href="http://modulardata.io/privacy-policy.html" target="_blank">Privacy Policy</a>,<br/>including use of cookies.</small></p>
		                            </div>
	                            </fieldset>
	                        </form>
	                    </div>
	                </div>
	            </div>
	        </div>
	    </div>
		
	
    </body>
</html>