[#ftl]

<h1>This is a dashboard</h1>
<br/>
<br/>

<div class="row">
  [#list summary! as s]
  <div class="col-sm-4" style="padding-bottom:20px">
    <div class="card">
      <div class="card-body">
        <div class="row">
	        <div class="col">
	          <h5 class="card-title text-uppercase text-muted mb-0">${s.name!}</h5>
	          <span class="h2 font-weight-bold mb-0">${s.value!}</span>
	        </div>
	        <div class="col-auto">
	            <i class="fa-solid fa-${s.icon!}"></i>
	        </div>
	      </div>[#--  row --]
	      <p class="mt-3 mb-0 text-muted text-sm">
            <span class="text-[#if s.pc!<0 ]danger[#else]success[/#if] mr-2"><i class="fa fa-arrow-[#if s.pc!<0 ]down[#else]up[/#if]"></i> ${s.pc!}%</span>
            <span class="text-nowrap">Since last ${s.duration!}</span>
          </p>
      </div>
    </div>
  </div>
  [/#list]
</div>