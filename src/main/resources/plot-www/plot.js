var k = [];
for(var i = 0; i < 10; i++) {
    k[i] = Math.random() / Math.pow(100, i);
}
var sourceFunction = function(x){
    return x * Math.sin(x);
     /*
    var y = 0;
    for(var i = 0; i < 10; i++) {
        y += Math.pow(x, i) * k[i];
    }
    return y;
    */
}

var min = 0;
var max = 1000000000;
var pointsCount = 80; //width / pixelsBetweenPoints

var mobilecheck = function() {
  var check = false;
  (function(a,b){if(/(android|bb\d+|meego).+mobile|avantgo|bada\/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip(hone|od)|iris|kindle|lge |maemo|midp|mmp|mobile.+firefox|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\.(browser|link)|vodafone|wap|windows ce|xda|xiino/i.test(a)||/1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw\-(n|u)|c55\/|capi|ccwa|cdm\-|cell|chtm|cldc|cmd\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\-s|devi|dica|dmob|do(c|p)o|ds(12|\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\-|_)|g1 u|g560|gene|gf\-5|g\-mo|go(\.w|od)|gr(ad|un)|haie|hcit|hd\-(m|p|t)|hei\-|hi(pt|ta)|hp( i|ip)|hs\-c|ht(c(\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\-(20|go|ma)|i230|iac( |\-|\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |\/)|klon|kpt |kwc\-|kyo(c|k)|le(no|xi)|lg( g|\/(k|l|u)|50|54|\-[a-w])|libw|lynx|m1\-w|m3ga|m50\/|ma(te|ui|xo)|mc(01|21|ca)|m\-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\-2|po(ck|rt|se)|prox|psio|pt\-g|qa\-a|qc(07|12|21|32|60|\-[2-7]|i\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\-|oo|p\-)|sdk\/|se(c(\-|0|1)|47|mc|nd|ri)|sgh\-|shar|sie(\-|m)|sk\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\-|v\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\-|tdg\-|tel(i|m)|tim\-|t\-mo|to(pl|sh)|ts(70|m\-|m3|m5)|tx\-9|up(\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas\-|your|zeto|zte\-/i.test(a.substr(0,4)))check = true})(navigator.userAgent||navigator.vendor||window.opera);
  return check;
}
var mobile = mobilecheck();

function chart(f, canvas, options) {
    this.options = options;
    var context = canvas.getContext('2d');
    var pixelsBetweenPoints = 10;
    var height = canvas.height;
    var width = canvas.width;
    var labels = [];
    var dataPoints = [];
    var data = {
        labels: labels,
        datasets: [
            {
                label: "My First dataset",
                fillColor: "rgba(220,220,220,0.2)",
                strokeColor: "rgba(220,220,220,1)",
                pointColor: "rgba(220,220,220,1)",
                pointStrokeColor: "#fff",
                pointHighlightFill: "#fff",
                pointHighlightStroke: "rgba(220,220,220,1)",
                data: dataPoints
            }
        ]
    };
    this.zoom = 1;
    this.shift = 0;
    var o = this;
    var plot = function(response) {
        var numericScale = [];
        var dateFormat;
        var scaleLengthInS = pointsCount * o.zoom;
        if(scaleLengthInS <= 24 * 60 * 60) {
            dateFormat = "HH:MM:ss";
        } else if(scaleLengthInS <= 31 * 24 * 60 * 60) {
            dateFormat = "dd HH:MM";
        } else if(scaleLengthInS <= 365 * 24 * 60 * 60) {
            dateFormat = "mmm.dd HH";
        } else {
            dateFormat = "yyyy.mmm.dd";
        }
        for(var i = 0; i < response.length; i++){
            var point = response[i];
            var x = point.k;
            if(point.range) {
                labels[i] = "AVG(" + new Date(point.range.from * 1000).format("yyyy.mmm.dd HH:MM:ss") + " : " +
                    new Date(point.range.to * 1000).format("yyyy.mmm.dd HH:MM:ss") + ")";
            } else {
                labels[i] = new Date(x * 1000).format("yyyy.mmm.dd HH:MM:ss");
            }
            dataPoints[i] = point.v;
            numericScale[i] = x;
        }
        o.chart = o.chart || new Chart(context).Line(data, options);
        for(var i = 0; i < response.length; i++){
            var point = o.chart.datasets[0].points[i];
            point.value = dataPoints[i];
            point.label = labels[i];
            point.numericLabel = numericScale[i];
            point.index = i;
            o.chart.scale.xLabels[i] = i%5==0? new Date(numericScale[i] * 1000).format(dateFormat):"";
        }
        o.chart.update();
    }
    this.draw = function() {
            var from = Math.round(this.shift + 0 * this.zoom);
            var to = Math.round(this.shift + pointsCount * this.zoom);
            if(to > max) {
                console.log('To is too large: ' + to);
                to = max;
            }
            if(from < min) {
                console.log('From is too small: ' + from);
                from = min;
            }
            $.ajax({
                type: "POST",
                url: "http://54.171.138.160/sample/?query",
                data: JSON.stringify({$from: from, $to: to, $groupsCount: pointsCount}),
                contentType: "application/json",
                dataType: "json",
                async: true,
                success: plot
            });
       }
    this.throttledDraw = _.throttle(this.draw, mobile ? 300 : 100);
}

var largeCanvas = document.getElementById("large_canvas");
var largeChart = new chart(sourceFunction, largeCanvas, {
    pointDot : false,
    pointHitDetectionRadius : 1,
    animationSteps: mobile ? 10 : 30,
    scaleFontFamily: "Lucida Console, Monaco, monospace"
});
var smallChartZoom = 20;
var smallCanvas = document.getElementById("small_canvas");
var smallChart = new chart(sourceFunction, smallCanvas, {
    pointDot : false,
    pointHitDetectionRadius : 1,
    animation: false,
    selection: {from: {x: 0},
        to: {x: pointsCount - 1}},
    scaleFontFamily: "Lucida Console, Monaco, monospace",
    showTooltips: false
});

var update = function(shift, zoom, force) {
    var maxZoom = (max - min) / pointsCount;
    if(zoom > maxZoom) {
        zoom = maxZoom;
    } else if(zoom < 1) {
        zoom = 1;
    }
    if(!force && largeChart.zoom == zoom) {
        return;
    }
    var maxLargeShift = max - pointsCount * zoom;
    if(shift < min) {
        shift = min;
    } else if(shift > maxLargeShift) {
        shift = maxLargeShift;
    }
    largeChart.zoom = zoom;
    largeChart.shift = shift;
    smallChart.zoom = zoom * smallChartZoom;
    if(smallChart.zoom > maxZoom) {
        smallChart.zoom = maxZoom;
    }
    smallChart.shift = shift - pointsCount * zoom * smallChartZoom / 2;
    if(smallChart.shift < 0) {
        smallChart.shift = 0;
    }
    smallChart.options.selection.from.x = (shift - smallChart.shift) / smallChart.zoom;
    smallChart.options.selection.to.x = (shift + (pointsCount - 1)* zoom - smallChart.shift) / smallChart.zoom;
    largeChart.throttledDraw();
    smallChart.throttledDraw();
}
update(0, (max - min) / pointsCount);
var withinBorder = function(border, point) {
    return Math.abs(border.x - point.index) < 1;
}
smallCanvas.addEventListener("mousemove", function(e){
    var points = largeChart.chart.getPointsAtEvent(e);
    if(points.length == 0) {
        return;
    }
    var selection = smallChart.options.selection;
    var processBorder = function(border, xPredicate) {
        if(border.dragDrop) {
            var x = points[0].index;
            if(xPredicate(x)) {
                border.x = x;
            }
            return;
        }
        if(selection.from.dragDrop || selection.to.dragDrop) {
            return;
        }
        if(withinBorder(border, points[0])) {
            if(!border.selected) {
                border.selected = true;
                border.strokeStyle = "darkblue";
            }
        } else {
            if(border.selected) {
                border.selected = false;
                border.strokeStyle = null;
            }
        }
    }
    processBorder(selection.from, function(x){return x < selection.to.x - 4});
    processBorder(selection.to, function(x){return x > selection.from.x + 4});
    smallChart.draw();
});
smallCanvas.addEventListener("mousedown", function(e){
    var points = largeChart.chart.getPointsAtEvent(e);
    if(points.length == 0) {
        return;
    }
    var selection = smallChart.options.selection;
    if(withinBorder(selection.from, points[0])) {
        selection.from.dragDrop = true;
    } else if(withinBorder(selection.to, points[0])) {
        selection.to.dragDrop = true;
    }
});
smallCanvas.addEventListener("mouseup", function(e){
    var selection = smallChart.options.selection;
    if(selection.from.dragDrop) {
        var shift = selection.from.x * smallChart.zoom + smallChart.shift;
        var zoom = (selection.to.x * smallChart.zoom - shift + smallChart.shift) / (pointsCount - 1);
        update(shift, zoom, true);
    } else if(selection.to.dragDrop) {
        var zoom = (selection.to.x * smallChart.zoom - largeChart.shift + smallChart.shift) / pointsCount;
        update(largeChart.shift, zoom, true);
    }
    selection.from.dragDrop = false;
    selection.to.dragDrop = false;
    selection.from.selected = false;
    selection.from.strokeStyle = null;
    selection.to.selected = false;
    selection.to.strokeStyle = null;
    smallChart.draw();
});
largeCanvas.addEventListener("mousewheel", function(e){
    e.preventDefault();
    var points = largeChart.chart.getPointsAtEvent(e);
    if(points.length == 0) {
        return;
    }
    if(!e.wheelDeltaY) {
        return;
    }
    var s1 = largeChart.zoom;
    var s2 = 1 / Math.pow(2, e.wheelDeltaY / 1000);
    var xf2 = points[0].numericLabel;
    var zoom = s1 * s2;
    var shift = xf2 * (1 - s2) + s2 * largeChart.shift;
    update(shift, zoom);
});

if(mobile) {
    var hammertime = new Hammer.Manager(largeCanvas,
        {recognizers: [
                // RecognizerClass, [options], [recognizeWith, ...], [requireFailure, ...]
                [Hammer.Rotate],
                [Hammer.Pinch, { enable: true }, ['rotate']]
        ]});
    hammertime.get('pinch').set({ enable: true });
    hammertime.on('pinch', function(e) {
        console.log(e);
        e.srcElement = largeCanvas;
        e.clientX = e.center.x;
        e.clientY = e.center.y;
        e.preventDefault();
        var points = largeChart.chart.getPointsAtEvent(e);
        if(points.length == 0) {
            return;
        }
        if(!e.scale) {
            return;
        }
        var s1 = largeChart.zoom;
        var s2 = 1 / e.scale;
        var xf2 = points[0].numericLabel;
        var zoom = s1 * s2;
        var shift = xf2 * (1 - s2) + s2 * largeChart.shift;
        update(shift, zoom);
    });
}