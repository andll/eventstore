var express = require('express');
var app = express();
app.use(express.static("src/main/resources/plot-www"));
var server = require('http').createServer(app);
server.listen(8080);