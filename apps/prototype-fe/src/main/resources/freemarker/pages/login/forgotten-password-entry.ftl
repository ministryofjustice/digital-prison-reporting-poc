[#ftl]
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en" xml:lang="en">
	<head>
		<title>Modular Data : Reset Password</title>
	</head>
	<body>
		
		[#assign hasPasswordError = (errors!?size > 0 && errors['password']!?length > 0)/]
		[#assign hasConfirmError = (errors!?size > 0 && errors['confirm']!?length > 0)/]
		<div class="container">
	        <div class="row">
	            <div class="col-md-5 col-md-offset-4">
		            <div class="login-panel panel panel-default">
	                    <div class="panel-heading">
	                        <h3 class="panel-title">Create a new Password</h3>
	                    </div>
	                    <div class="panel-body">
					        <form method="post" action="/password-reset" id="password-reset" onsubmit="javascript: return valid.check(this);" class="panel-body wrapper-lg"> 
					          <p class="text-muted text-center"><small>Please enter a new password</small></p>
					          <div class="form-group[#if hasPasswordError] has-error[/#if]">
                            		<label class="control-label" for="password">[#if hasPasswordError]${errors['password']}[#else]Password[/#if]</label>
                                	<input class="form-control" placeholder="Your password (8 or more characters)" id="password" name="password" type="password" value="${password!}">
                              </div>
					          <div class="form-group[#if hasConfirmError] has-error[/#if]">
                            		<label class="control-label" for="confirm">[#if hasConfirmError]${errors['confirm']}[#else]Confirm Password[/#if]</label>
                                	<input class="form-control" placeholder="Confirm password (8 or more characters)" id="confirm" name="confirm" type="password" value="${confirm!}">
                              </div>
					          <input type="hidden" name="userId" value="${userId}" id="userId" />
					          <button type="submit" class="btn btn-lg btn-success btn-block">Reset Password</button>
					          <div class="foot">
	                            <br/>
                            	<p class="text-muted text-center"><small>Do not have an account?</small></p>
                            	<a class="btn btn-sm btn-primary btn-block" href="/signup">Create an account</a>
                              </div>
					        </form>
						  </div>
					</div>
			     </div>
			 </div>
	    </div>   
    </body>
</html>