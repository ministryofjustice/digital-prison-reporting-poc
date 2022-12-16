[#ftl]
<head>
	<script src="assets/js/echarts.js"></script>
	<script src="https://cdn.datatables.net/1.12.1/js/jquery.dataTables.min.js"></script>
	<script src="assets/plugins/bootstrap-datepicker/bootstrap-datepicker.min.js"></script>
	<link href="assets/plugins/bootstrap-datepicker/bootstrap-datepicker.css" rel="stylesheet"/>
	
	<script>
		
		var chart;
		var data = { 'Under 21':0, '21-25':0, '26-30':0, '31-35':0, '36-40':0, '41-45':0, '46-50':0, '51-55':0, '56-60':0, '61-65':0, '66-70':0, 'Over 70':0 };
		
		$(document).ready(function() {
			initialiseChart();
			applyQuery();
			initialiseDatePicker();
		});
		
		function updateState(anchor) {
			var currentAnchor = window.location.hash.substr(1);
			var nextUrl = document.URL.replace(/#.*$/, "");
			var nextTitle = $('title').text();
			var nextState = {};
			window.history.pushState(nextState, nextTitle, nextUrl + "#" + anchor);
		}
		
		function initialiseChart() {
			chart = echarts.init(document.getElementById('chart'));
			
			var option = {
				title: {},
				tooltip: {},
				legend: { data: ['incidents']},
				xAxis: { data: Object.keys(data) },
				yAxis: {},
				series: [{ name: 'incidents', type: 'bar', data: Object.values(data)}]
			}
			chart.setOption(option);
		}
		
		function initialiseDatePicker() {
			$('.input-daterange').datepicker({
			    format: "dd/mm/yyyy"
			});
		}
		
		function applyQuery() {
			var params = $("#form").serialize();
			// apply query
			$.post("/app/interactive", params, function(data) {
				var response = JSON.parse(data);
				console.log(response);
				makeChart(response, "chart");
				makeTable(response, "table");
				updateLabels();
				
			});
		}
		
		function updateLabels() {
		
			
			$("#d_type").text($("#type").val().length === 0 ? "All" : $("#type").val());
			$("#d_establishment").text($("#establishment").val().length === 0 ? "All" : $("#establishment").val());
			
			if($("#start").val().length === 0 && $("#end").val().length === 0)
				$("#d_date_range").text("");
			else if ($("#start").val().length === 0 )
				$("#d_date_range").text("To " + $("#end").val());
			else if ($("#end").val().length === 0 )
				$("#d_date_range").text("From " + $("#start").val());
			else
				$("#d_date_range").text($("#start").val() + " to " + $("#end").val());
				
			var anchor = ( $("#type").val().length === 0 ? "" : "t=" + $("#type").val() + "&" ) +
						 ( $("#start").val().length === 0 ? "" : "ds=" + $("#start").val() + "&" ) +
						 ( $("#end").val().length === 0 ? "" : "de=" + $("#end").val() + "&" ) +
						 ( $("#establishment").val().length === 0 ? "" : "e=" + $("#establishment").val());
			updateState(anchor);
		}
		
		function makeChart(json, target) {
			// clear data
			for(const [key,value] of Object.entries(data)) {
				data[key] = 0;
			}
			
			$.each(json, function() {
				if(!(this.age_category in data)) {
					data[this.age_category] = 1;
				} else {
					data[this.age_category] += 1;
				}              
		    });
		    chart.setOption({
		    	xAxis: { data: Object.keys(data), axisLabel: { interval: 0 }}, 
		    	series: [{ name: 'incidents', data : Object.values(data) }]
		    });
		}
		
		function makeTable(json, target) {
			var tbl_body = "";
			var tbl_head = "";
    		var odd_even = false;
			$.each(json, function() {
		        var tbl_row = "";
		        tbl_head = "";
		        $.each(this, function(k , v) {
		        	tbl_head +="<th scope='col'>"+k+"</th>";
		        	if(k == 'type') {
		        		tbl_row += "<td>Use of Force</td>"; // should translate from code to human-readable version
		        	} else {
		            	tbl_row += "<td>"+v+"</td>";
		            }
		        });
		        tbl_body += "<tr class=\""+( odd_even ? "odd" : "even")+"\">"+tbl_row+"</tr>";
		        odd_even = !odd_even;               
		    });
		    
		    // $("#" + target +" thead").html("<tr>"+tbl_head+"</tr>");
		    $("#" + target +" tbody").html(tbl_body);
		}
	</script>
</head>
<h1>Incidents by Age Group</h1>
<br/>
<br/>
[#-- CHART --]
<div class="row">
	<div class="col-md-3">
		<div class="form-group row">
			<label for="d_type">Incident Type :&nbsp;</label>
			<span id="d_type">All</span>
		</div>
		<div class="form-group row">
			<label for="d_date_range">Date Range :&nbsp;</label>
			<span id="d_date_range"></span>
		</div>
		<div class="form-group row">
			<label for="d_establishment">Incident Establishment :&nbsp;</label>
			<span id="d_establishment">All</span>
		</div>
	</div>
	<div class="col-md-9" style="min-height: 400px" id="chart"></div>
</div>
<br/>
[#-- INTERACTIVE DATE SELECTORS --]
<form id="form" action="#">
<div class="form-row align-items-center">
	[#-- Date Range --]
	<div class="col-md-7">
		<div class="form-group">
			<label for="dates">Date Range</label>
			<div class="input-daterange input-group" id="dates">
			    <input type="text" class="input-sm form-control" id="start" name="start" />
			    <span class="input-group-addon">&nbsp;to&nbsp;</span>
			    <input type="text" class="input-sm form-control" id="end" name="end" />
			</div>
		</div>
	</div>
	[#--  Establishment --]
	<div class="col-md-2">
		<div class="form-group">
			<label for="establishment">Incident establishment</label>
			<select class="form-control" name="establishment" id="establishment">
				<option value="">All</option>
			[#list establishments?keys as e]
				<option value="${establishments[e]!}">${establishments[e]!}</option>
			[/#list]
			</select>
		</div>
	</div>
	
	<div class="col-md-2">
		<div class="form-group">
			<label for="type">Incident type</label>
			<select class="form-control" name="type" id="type">
				<option value="">All</option>
			[#list types?keys as e]
				<option value="${e}">${types[e]!}</option>
			[/#list]
			</select>
		</div>
	</div>
	<div class="col-auto">
		<div class="form-group">
			<label>&nbsp;</label>
			<div><a href="#" onclick="applyQuery(); return false;" class="btn btn-primary align-bottom">Apply</a></div>
		</div>
	</div>
</div>
</form>
<br/>
<br/>
[#-- Table --]
<div class="row">
	<table id="table" class="table table-striped table-bordered" style="width:100%">
		<thead class="thead-dark">
			<th width="10%" scope='col'>Incident Number</th>
			<th width="20%" scope='col'>Date of Incident</th>
			<th width="15%" scope='col'>Incident Type</th>
			<th width="15%" scope='col'>NOMS Number</th>
			<th width="20%" scope='col'>Incident Establishment</th>
			<th width="10%" scope='col'>Age at Time of Incident</th>
			<th width="10%" scope='col'>Age Grouping</th>
		</thead>
		<tbody></tbody>
	</table>
</div>
[#if exception??]
<div class="alert alert-danger" role="alert">
  Unable to connect to data serving layer
  <!-- 
  ${exception}
  -->
</div>
[/#if]

