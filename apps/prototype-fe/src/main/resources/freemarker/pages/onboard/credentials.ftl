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
	</head>
	<body>
	
		[#assign hasUsernameError = (errors!?size > 0 && errors['username']!?length > 0)/]
		[#assign hasPasswordError = (errors!?size > 0 && errors['password']!?length > 0)/]
		
		<div class="container">
	        <div class="row">
	            <div class="col-md-6 col-md-offset-4">
	                <div class="login-panel panel panel-default">
	                    <div class="panel-body">
	                    	<h2 style="text-align:center">Create your <strong>free account</strong></h2>
	                    	<h4 style="text-align:center;color:#b7c4cd">It's simple and takes less than a minute</h4>
	                        <form role="form" action="/register/1" method="post" onsubmit="javascript: return valid.check(this);">
	                            <fieldset>
	                                <div class="form-group[#if hasUsernameError] has-error[/#if]">
	                                	[#if hasUsernameError]<label class="control-label" for="username">${errors['username']}</label>[/#if]
	                                    <input class="form-control" placeholder="Your email" id="username" name="username" type="email" value="${username!}" autofocus>
	                                </div>
	                                <div class="form-group[#if hasPasswordError] has-error[/#if]">
	                                	[#if hasPasswordError]<label class="control-label" for="password">${errors['password']}</label>[/#if]
	                                    <input class="form-control" placeholder="Your password (8 or more characters)" id="password" name="password" type="password" value="${password!}">
	                                </div>
		                            <button type="submit" class="btn btn-lg btn-success btn-block">Next >></button>
	                                <div class="foot">
	                                	<br/>
	                                	<p class="text-center"><a href="/authenticate"><small>Already registered?</small></a></p>
	                                	<p class="text-center"><small style="color:#b7c4cd">By signing up, you agree to our Terms of Service and Privacy Policy, including use of cookies.</small></p>
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