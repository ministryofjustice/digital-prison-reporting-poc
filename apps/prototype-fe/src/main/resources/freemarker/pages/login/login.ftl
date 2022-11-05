[#ftl]
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en" xml:lang="en">
	<head>
		<title>Login to Modular Data Discover</title>
		<style>
			body {
				background-color: #FFFFFF !important;
			}
		</style>
	</head>
	<body>
		[#assign hasLoginError = (Session.SPRING_SECURITY_LAST_EXCEPTION?? && Session.SPRING_SECURITY_LAST_EXCEPTION.message?has_content)/]
	
		<div class="container">
	        <div class="row">
	            <div class="col-md-5 col-md-offset-4">
	                    	
	                <div class="login-panel panel panel-default">
	                    <div class="panel-body">
	                    	<h2 style="text-align:center"><strong>Modular Data</strong></h2>
	                    	<h4 style="text-align:center;color:#b7c4cd">Log in</h4>
	                        <form role="form" action="login" method="post" onsubmit="javascript: return valid.check(this);">
	                            <fieldset>
	                                <div class="form-group[#if hasLoginError] has-error[/#if]">
	                                	[#if hasLoginError]
										    <label class="control-label" for="username">The username or password you have entered is invalid</label>
										[/#if]
	                                    <input class="form-control" placeholder="test@example.com" id="username" name="username" type="email" autofocus>
	                                </div>
	                                <div class="form-group[#if hasLoginError] has-error[/#if]">
	                                    <input class="form-control" placeholder="Password" id="password" name="password" type="password" value="">
	                                </div>
	                                <div class="checkbox text-center">
	                                    <label>
	                                        <input name="remember-me" type="checkbox" value="checked">Remember Me
	                                    </label>
	                                </div>
		                            <button type="submit" class="btn btn-lg btn-success btn-block">Log in</button>
	                                <div class="foot">
	                                	<br/>
	                                	<p class="text-center"><a href="/password-retrieve"><small>Forgot password?</small></a></p>
		                            	<p class="text-center">Do not have an account?</p>
		                            	<a class="btn btn-sm btn-primary btn-block" href="/signup">Create an account</a>
		                            </div>
	                            </fieldset>
	                        </form>
	                    </div>
	                    [#--  include "/social/social.ftl"/ --]
	                </div>
	            </div>
	        </div>
	    </div>
		
	
    </body>
</html>