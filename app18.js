#!/usr/bin/env node
'use strict';

const express = require('express');
const app = express();

//liste des packages
var bodyParser = require('body-parser'),
  session = require('express-session'),
  favicon = require('serve-favicon'),
  compress = require('compression'),
  methodOverride = require('method-override'),
  cookieParser = require('cookie-parser'),
  helmet = require('helmet'),
  path = require('path'),
  url = require('url'),
  sql = require("mssql"),
  nodemailer = require('nodemailer');  

//routes et dossiers publics  /static files
var options = {
  dotfiles: 'ignore',
  etag: false,
  extensions: ['htm', 'html'],
  index: false,
  maxAge: '1d',
  redirect: false,
  setHeaders: function (res, path, stat) {
    res.set('x-timestamp', Date.now())
  }
};

app.use('/', express.static(path.resolve('/vue'), options));
app.use('/', express.static(path.resolve('/public'), options)); 

//liste de middlewares
app.use(bodyParser.urlencoded({extended: false}));
app.use(bodyParser.json());
app.use(methodOverride());
app.use(cookieParser());
app.use(helmet());
app.disable('x-powered-by');

// indiquer la route pour les APIs à appeler
var router01= require('./routes/app20180208')
app.use('/', router01);

//servir la page d'accueil
var konek=0;
app.get('/', function(req, res){
	konek++;
	console.log("Connexion no : ", konek);
});

//Mettre le serveur d'application en marche
var server = app.listen(3059, function () {
    console.log('Serveur en marche ...\nPour s\'y connecter, visiter 172.30.101.101:3059');
	;
});