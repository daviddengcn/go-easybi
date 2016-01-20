package bi

const gIndexHtml string = `
<html>
<head>
<title>{{.Name}}</title>
<script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
<script type="text/javascript">
	google.charts.load('current', {'packages':['line', 'corechart']});
	google.charts.setOnLoadCallback(drawChart);
	function drawChart() {
		var data = new google.visualization.DataTable();
		data.addColumn('string', 'Date');
		data.addColumn('number', 'Count');
		{{if .Name}}
		{{range $idx, $c := .Data }}
		  data.addRows([
		    ['{{$c.Label}}', {{$c.Count}}],
		  ]);
		{{end}}
		{{end}}

		// Set chart options
		var options = {
			chart: {
				'title': '{{.Name}}',
			},
			'title': '{{.Name}}',
			'curveType': 'function',
			'pointSize': 5,
			'width': '100%',
			'height': 600
		};

		var chart = new google.visualization.LineChart(document.getElementById('chart_div'));
		chart.draw(data, options);
	}
</script>
<style>

body {
	background: #eee;
	border: 0px;
    border-collapse: collapse;
	margin: 0px;
}

table.frame {
	width: 100%;
	height: 100%;
}
td.left-panel {
	background: #eee;
	width: 200px;
}
div.left-inner {
	height: 100%;
    overflow: scroll;
}
div.left-inner ul {
	padding-left: 20px;
}
td.right-panel {
	background-color: white;
}
div.right-inner {
	height: 100%;
    overflow: scroll;
}
</style>
<body>

<table class="frame">
<tbody><tr>
<td class="left-panel"><div class="left-inner">
<div>
	<a href="?name={{.Name}}&type=daily">daily</a>
	<a href="?name={{.Name}}&type=weekly">weekly</a>
	<a href="?name={{.Name}}&type=monthly">monthly</a>
	<a href="?name={{.Name}}&type=yearly">yearly</a>
</div>
<ul>
	{{$type := .Type}}
	{{range $sub := .Names}}
		{{$prefix1 := $sub.Prefix}}
		<li>{{if .Included}}<a href="?name={{$prefix1}}&type={{$type}}">{{end}}{{.Prefix}}{{if .Included}}</a>{{end}}</li>
		{{if .Subs}}
			<ul>{{range $sub := .Subs}}
			    {{$prefix2 := print $prefix1 "." $sub.Prefix}}
				<li>{{if .Included}}<a href="?name={{$prefix2}}&type={{$type}}">{{end}}{{.Prefix}}{{if .Included}}</a>{{end}}
					{{if .Subs}}
						<ul>{{range $sub := .Subs}}
						    {{$prefix3 := print $prefix2 "." $sub.Prefix}}
							<li>{{if .Included}}<a href="?name={{$prefix3}}&type={{$type}}">{{end}}{{.Prefix}}{{if .Included}}</a>{{end}}</li>
						{{end}}</ul>
					{{end}}
				</li>
			{{end}}</ul>
		{{end}}
	{{end}}
</ul>
</div></td>
<td class="right-panel"><div class="right-inner">
<div id="chart_div"></div>
</div></td>
</tr></tbody>
</table>
`
