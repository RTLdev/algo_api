#!/usr/bin/env node
'use strict';

//Liste de librairies
var express = require('express');
var router = express.Router();

//Liste de middlewares
const path = require('path');
const fs = require('fs');
const Axios = require('axios');

var datetime = require('date-time');
var session = require('express-session'); 

var compression = require('compression');
router.use(compression()); 

var Promise = require('promise');
const async=require('async');
var omit = require('object.omit');

//Browser details
const { detect } = require('detect-browser');
const browser = detect();
const http = require('http');
const url = require('url');

let f01=ftimestamp(new Date()); 

//Quelques constantes et LISTE DE VARIABLES GLOBALEMENT DÉCLARÉES POUR FIN DE REQUETES

//globals
var DTHRDebut, DTHRFin, DATEDebut, DATEFin, delai; //date 
var temps, LaFin, LeJour, Moment, DernierJourTraite; //date
var Fonctionnement_auto, NbDeTraitement=0, ordre_validation; //Byte
var DATETxtDebut, DATETxtFin,ANNEEopus2, ANNEEopus4, JourSemaine; //String
var Code_imputation_debarquement;

//'prix d'un passage comptant à bord sans correspondance 
//à modifier lors d'un changement de tarif
const passage_unitaire = 3.50;

//fonction mise en forme (aaaa-mm-jj) d'une date qq
function formatdate(qq){
	let dqq=new Date(qq);
	let jj = dqq.getDate();
	let mm = dqq.getMonth()+1;
	if(mm<10) mm='0'+mm;
	if(jj<10)jj='0'+jj;
	let aa= dqq.getFullYear()+'-'+mm+'-'+jj;
	return aa;
}

//fonction mise en forme (aaaammjj) d'une date qq
function formatdate01(qq){
	let dqq=new Date(qq);
	let jj = dqq.getDate();
	let mm = dqq.getMonth()+1;
	if(mm<10) mm='0'+mm;
	if(jj<10)jj='0'+jj;
	let aa= ''+dqq.getFullYear()+mm+jj;
	return aa;
}

//fonction mise en forme (aaaa-mm-jj hh:mm:ss) du timestamp qq
function ftimestamp(qq){
	let dqq=new Date(qq), jj = dqq.getDate(), mm = dqq.getMonth()+1;
	let hh = dqq.getHours(), nn = dqq.getMinutes(), ss = dqq.getSeconds();
	if(mm<10) mm='0'+mm;
	if(jj<10) jj='0'+jj;
	if(hh<10) hh='0'+hh;
	if(nn<10) nn='0'+nn;
	if(ss<10) ss='0'+ss;
	let aa= dqq.getFullYear()+'-'+mm+'-'+jj+' '+hh+':'+nn+':'+ss;
	return aa;
}

//calculer différence en jours entre deux dates 
function diffDate(d01, d02){
	let ujour = 24*60*60*1000, d1=new Date(d01), d2= new Date(d02);
	let diff = Math.round(Math.abs((d1.getTime() - d2.getTime())/(ujour)));
	return diff;
}

//Fonction pour convertir une heure t (hh:mm:ss) en secondes
function h2sec(g){
	let h= new Date(g);
	let ss =(h.getHours())*3600 + (h.getMinutes())*60 + h.getSeconds();
	return ss; 
}

//fonction delay comme dans C
function delay(k){
	setTimeout( ()=>{}, k);
}

//Date et heure actuelle
const dd = new Date();  

/////////////////////////////////////////////////////////
//          API pour MS SQL SERVER BDD       		  //   
///////////////////////////////////////////////////////

//informations de connection au DBB MS SQL Server
const sql = require("mssql");

const options= {
	encrypt: true, //Use this if you're on Windows Azure
	requestTimeout:60000
}

const config = {
	user: 'Planifxxxxxxxxxxx',
	password: 'xxxxxxxxxx',
	//server: You can use 'localhost\\instance' to connect to named instance  --- 172.30.100.122
	server: 'SLSQL05\\Planif_works',
	database:'AchalDep' ,
	options: options
};

/////////////////////////////////////////////////////////
//              LISTE DES REQUETES SQL       		  //   
///////////////////////////////////////////////////////

// CREER LES TABLES TEMPORAIRES
let essai=0;
async function tableCreation(){
	await delay(300);
	console.log('\nPromise 6.3 -- '+formatdate(global.dd)+' Debut de la Création des tables temporaires');
	
	let arret_metro = " CREATE TABLE arret_metro(no_station	int, nom_station varchar(100), coord_x decimal, coord_y decimal, note varchar(100) ); "+
		" INSERT INTO [dbo].[arret_metro](no_station,nom_station,coord_x,coord_y) "+
		" VALUES (168,'Radisson',301688,5049976),(150,'Papineau',300730,5042642),(252,'Bonaventure',299594,5039824),(454,'Longueuil',303095,5042751); ",
						
	arrets_possibles="CREATE TABLE arrets_possibles(ID_D28_S28 nvarchar(255), identification	varchar(100), no_arret decimal, no_arret_emb	decimal, no_arret_deb decimal,"+
		"direction	char(1), position decimal, position_boucle decimal, position_arret_emb decimal, coord_x decimal, coord_x_emb decimal, coord_y decimal, coord_y_emb decimal, nombre_occ decimal,"+
		"proximite_temporelle decimal, proximite_arret	decimal	); CREATE INDEX ID ON arrets_possibles (ID_D28_S28 ASC);",

	arrets_probables="CREATE TABLE arrets_probables("+
		" ID_D28_S28 nvarchar(255) NOT NULL,"+
		" proximite_arret decimal,"+
		" proximite_temporelle decimal,"+
		" nombre_occ decimal,"+
		" direction	char(1) NOT NULL,"+
		" PRIMARY KEY (ID_D28_S28, direction) with (IGNORE_DUP_KEY = ON));",

	BD_Valid_Metro = "CREATE TABLE BD_Valid_Metro("+
		" ID_D28_S28 nvarchar(255),"+
		" identification varchar(100),"+
		" date28 date,"+
		" seconde28 int,"+
		" ligne numeric(3,0),"+
		" station decimal,"+
		" ordre_valid_total	int,"+
		" coord_x decimal,"+
		" coord_y decimal,	"+
		" PRIMARY KEY (ID_D28_S28) with(IGNORE_DUP_KEY = ON));"+
		" CREATE INDEX date28 ON BD_Valid_Metro (date28 ASC);"+
		" CREATE INDEX seconde28 ON BD_Valid_Metro (seconde28 ASC);",

	boucle_course = "CREATE TABLE boucle_course("+
		"date_j	date,"+
		"assignation char(8),"+
		"type_service char(2),"+
		"voiture char(8),"+
		"ligne numeric(3,0),"+
		"hre_pre_dep char(8),"+
		"dep_suivant_boucle	char(8),"+
		"direction char(1),"+
		"trace numeric(3,0),"+
		"pos_arr numeric(5,0)	);"+
		"CREATE INDEX hre_pre_dep ON  boucle_course (hre_pre_dep ASC);"+
		"CREATE INDEX hre_pre_dep1 ON  boucle_course (dep_suivant_boucle ASC);"+
		"CREATE INDEX voiture ON  boucle_course (voiture ASC);",

	bus_course = "CREATE TABLE bus_course("+
		"No	int,"+
		"date_j	date,"+
		"no_bus	char(8),"+
		"no_bus_sdap varchar(100),"+
		"voiture char(8),"+
		"assignation varchar(100),"+
		"type_service char(2),"+
		"ligne decimal,"+
		"hre_deb_voiture_veh char(8),"+
		"hre_fin_voiture_veh char(8),"+
		"hre_pre_dep char(8),"+
		"hre_pre_arr char(8),"+
		"direction	char(1),"+
		"trace varchar(100),"+
		"periode char(2),"+
		"sec_dep_course_pre	int,"+
		"sec_arr_course_pre	int,"+
		"sec_dep_course_reel int,"+
		"sec_arr_course_reel int,"+
		"sec_deb_voiture int,"+
		"sec_fin_voiture int,"+
		"pos_course	varchar(100)	); "+
		"CREATE INDEX arr_pre ON  bus_course (sec_arr_course_pre ASC); "+
		"CREATE INDEX arr_ree ON  bus_course (sec_arr_course_reel ASC); "+
		"CREATE INDEX bus ON  bus_course (no_bus ASC); "+
		"CREATE INDEX cle ON  bus_course (date_j ASC, no_bus ASC, hre_pre_dep ASC, voiture ASC); "+
		"CREATE INDEX voiture ON  bus_course (voiture ASC); "+
		"CREATE INDEX date_j ON  bus_course (date_j ASC); "+
		"CREATE INDEX dep_pre ON  bus_course (sec_dep_course_pre ASC); "+
		"CREATE INDEX dep_ree ON  bus_course (sec_dep_course_reel ASC); "+
		"CREATE INDEX ligne ON  bus_course (ligne ASC);",

	calendrier=	" CREATE TABLE calendrier( "+
		" date_j date not null PRIMARY KEY , "+
		" assignation	char(8), "+
		" date_num	varchar(100) , "+
		" annee	int, "+
		" mois	int, "+
		" jour	int, "+
		" type_service	varchar(2), "+
		" type_service2	varchar(100), "+
		" relache_scolaire	varchar(100)	); "+
		" CREATE INDEX date_num ON  calendrier(date_num ASC); "+
		" CREATE INDEX type_service ON  calendrier(type_service ASC); "+
		" declare @sdate datetime=convert(date,'20150101') "+
		" declare @edate datetime=convert(date,'20211231') "+
		" insert into calendrier "+
		" select DATEADD(d,number,@sdate), null as assignation, null as date_num, "+
		" null as annee, null as mois, null as jour, null as type_service, "+
		" null as type_service2, null as relache_scolaire from master..spt_values where type='P' and number<=datediff(d,@sdate,@edate) ;"+
		" Update calendrier "+
		" set date_num = SUBSTRING(CAST(date_j AS VARCHAR(255)),1,4)+SUBSTRING(CAST(date_j AS VARCHAR(255)),6,2)+SUBSTRING(CAST(date_j AS VARCHAR(255)),9,2), "+
		" annee= datepart(YYYY, date_j), mois=datepart(MM, date_j), jour=datepart(DD, date_j) "+
		" from calendrier;",
		
	CAP_GFI_temp="CREATE TABLE CAP_GFI_temp("+
		"ID_D28_S28	nvarchar(255),"+
		"identification	varchar(100),"+
		"code_titre	nvarchar(5),"+
		"nom_titre varchar(100),"+
		"date24	date,"+
		"date28	date,"+
		"heure24 time,"+
		"seconde28 int,"+
		"Date_complet_28 datetime,"+
		"assignation char(8),"+
		"type_service varchar(2),"+
		"no_bus_CAP	varchar(100),"+
		"no_bus	varchar(100),"+
		"voiture_CAP varchar(100),"+
		"voiture char(8),"+
		"lig_cap decimal,"+
		"ligne decimal,"+
		"periode varchar(100),"+
		"hre_pre_dep char(8),"+
		"hre_pre_arr char(8),"+
		"direction	char(1),"+
		"trace varchar(100),"+
		"pos_course	decimal,"+
		"type_val varchar(100),"+
		"ordre_valid decimal,"+
		"trajet	decimal,"+
		"ordre_valid_total int,"+
		"chaine_val	int,"+
		"nb_montants decimal,"+
		"no_arret_emb decimal,"+
		"no_arret_deb decimal,"+
		"position_arret_emb	decimal,"+
		"coord_x_emb decimal,"+
		"coord_y_emb decimal,"+
		"source_imput varchar(100),"+
		"imput_emb decimal,"+
		"imput_deb decimal	);"+
		"CREATE INDEX derniere_valid ON CAP_GFI_temp(identification ASC, ordre_valid_total ASC);"+
		"CREATE INDEX id_arret_e_ligne_dir ON CAP_GFI_temp(identification ASC, no_arret_emb ASC, ligne ASC, direction ASC);"+
		"CREATE INDEX id_ligne_dir ON CAP_GFI_temp(identification ASC, ligne ASC, direction ASC);"+
		"CREATE INDEX imput_deb	ON CAP_GFI_temp(imput_deb ASC);"+
		"CREATE INDEX ligne_cap ON CAP_GFI_temp(lig_cap ASC);"+
		"CREATE INDEX no_bus_cap ON CAP_GFI_temp(no_bus_CAP ASC);"+
		"CREATE INDEX seconde28	ON CAP_GFI_temp(seconde28 ASC);"+
		"CREATE INDEX voiture_cap ON CAP_GFI_temp(voiture_CAP ASC);",

	chaine_validation="CREATE TABLE chaine_validation("+
		"No	int,"+
		"ordre_valid int,"+
		"seconde28	int,"+
		"identification	varchar(100),"+
		"date28	date,"+
		"no_bus_cap	varchar(100),"+
		"hre_pre_dep varchar(100),"+
		"chaine_val	int	);",

	chaine_validation2="CREATE TABLE chaine_validation2("+
		"No	int,"+
		"ordre_valid int,"+
		"seconde28 int,"+
		"identification	varchar(100),"+
		"date28	date,"+
		"no_bus_cap	varchar(100),"+
		"hre_pre_dep varchar(100),"+
		"chaine_val	int	);",

	chaine_validation3 = "CREATE TABLE chaine_validation3("+
		"No	int,"+
		"ordre_valid int,"+
		"seconde28 int,"+
		"identification	varchar(100),"+
		"date28	date,"+
		"no_bus_cap	varchar(100),"+
		"hre_pre_dep varchar(100),"+
		"chaine_val	int	);",

	dernieres_validations ="CREATE TABLE dernieres_validations("+
		"ID_D28_S28	nvarchar(255),"+
		"date28	date,"+
		"identification	varchar(100),"+
		"voiture varchar(100),"+
		"hre_pre_dep varchar(100),"+
		"no_arret_emb decimal,"+
		"position_arret_emb	decimal,"+
		"ligne	decimal,"+
		"direction	char(1),"+
		"trace	decimal	);",

	dico_titre = "CREATE TABLE dico_titre(titre_nom	varchar(100), Prd_Numero nvarchar(5));"+
		" INSERT INTO dico_titre(titre_nom ,  Prd_Numero) VALUES "+
		" ('OPUS-TRAM1-Mensuel-O ',  '1'),('TRAM4-Abonnement-R ',  '100'),('TRAM5-Abonnement-R ',  '101'),"+
		" ('TRAM6-Abonnement-R ',  '102'),('TRAM7-Abonnement-R ',  '103'),('TRAM8-Abonnement-R ',  '104'),"+
		" ('RTL-SOLO-12 BILL.ORD ',  '12BOJ'),('Solo-TRAM1-Carnet-O ',  '13'),('Solo-TRAM2-Carnet-O ',  '14'),"+
		" ('Solo-TRAM3-Carnet-O ',  '15'),('OPUS-ESSAI-BILLET ',  '159'),('Solo- 1 COURTOISIE-B ',  '160'),"+
		" ('OPUS-COURTOISIE-B ',  '161'),('Solo- 2 COURTOISIE-B ',  '162'),('Solo- 3 COURTOISIE-B ',  '163'),"+
		"  ('Solo- 4 COURTOISIE-B ',  '164'),('Solo- 5 COURTOISIE-B ',  '165'),('Solo- 6 COURTOISIE-B ',  '166'),"+
		" ('Solo- 7 COURTOISIE-B ',  '167'),('Solo- 8 COURTOISIE-B ',  '168'),('Solo- 9 COURTOISIE-B ',  '169'),"+
		" ('Solo-10COURTOISIE-B ',  '170'),('OPUS-AQLPA-TR ',  '171'),('OPUS-EVENEMENT ',  '172'),"+
		" ('EVSMV-BILLET ',  '174'),('ESSAI-BILLET ',  '175'),('OPUS-PROMO-2BILLETS ',  '176'),"+
		" ('RTL-SOLO-18 BILL.ORD ',  '18BOJ'),('RTL-SOLO-1 BILL.ORD ',  '1BOCJ'),('RTL-SOLO-1 jour-24hr ',  '1jour'),"+
		" ('RTL-1 passage ',  '1pacj'),('RTL-1 passage ',  '1pass'),('RTL-SOLO-1 soir ',  '1soir'),"+
		" ('OPUS-TRAM2-Mensuel-O ',  '2'),('Solo-TRAM1-Billet-O ',  '20'),('Employé/Retraité-OPT ',  '200'),"+
		" ('Employé/Retraité-AMT ',  '201'),('Employé/Retraité-CIT ',  '202'),('Chauffeur - CIT ',  '203'),"+
		" ('Solo-TRAM2-Billet-O ',  '21'),('Solo-TRAM3-Billet-O ',  '22'),('RTL-SOLO-24 BILL.ORD ',  '24BOJ'),"+
		" ('OPUS-TRAM1-Mensuel-E ',  '27'),('OPUS-TRAM2-Mensuel-E ',  '28'),('OPUS-TRAM3-Mensuel-E ',  '29'),"+
		" ('RTL-SOLO-2 BILL.ORD ',  '2BOCJ'),('RTL-2 passages ',  '2pacj'),('OPUS-TRAM3-Mensuel-O ',  '3'),"+
		" ('OPUS-TRAM4-Mensuel-E ',  '30'),('OPUS-TRAM5-Mensuel-E ',  '31'),('OPUS-TRAM6-Mensuel-E ',  '32'),"+
		" ('OPUS-TRAM7-Mensuel-E ',  '33'),('OPUS-TRAM8-Mensuel-E ',  '34'),('OPUS-TRAM1-Mensuel-R ',  '39'),"+
		" ('RTL-3 passages ',  '3pacj'),('OPUS-TRAM4-Mensuel-O ',  '4'),('OPUS-TRAM2-Mensuel-R ',  '40'),"+
		" ('OPUS-TRAM3-Mensuel-R ',  '41'),('OPUS-TRAM4-Mensuel-R ',  '42'),('OPUS-TRAM5-Mensuel-R ',  '43'),"+
		" ('OPUS-TRAM6-Mensuel-R ',  '44'),('OPUS-TRAM7-Mensuel-R ',  '45'),('OPUS-TRAM8-Mensuel-R ',  '46'),"+
		" ('RTL-OPUS-RÉD-4M ',  '4MRÉD'),('RTL-4 passages ',  '4pacj'),('OPUS-TRAM5-Mensuel-O ',  '5'),"+
		" ('OPUS-TRAM1-Carnet-R ',  '51'),('OPUS-TRAM2-Carnet-R ',  '52'),('OPUS-TRAM3-Carnet-R ',  '53'),"+
		" ('OPUS-TRAM1-Billet-R ',  '58'),('OPUS-TRAM2-Billet-R ',  '59'),('OPUS-TRAM6-Mensuel-O ',  '6'),"+
		" ('OPUS-TRAM3-Billet-R ',  '60'),('OPUS-TRAM1-Carnet-O ',  '65'),('OPUS-TRAM2-Carnet-O ',  '66'),"+
		" ('OPUS-TRAM3-Carnet-O ',  '67'),('RTL-OPUS-6 BILL.ORD. ',  '6BO'),('RTL-SOLO-6 BILL.ORD ',  '6BOCJ'),"+
		" ('RTL-OPUS-6 BILL.RÉD. ',  '6BR'),('RTL-OPUS-6MOISRED ',  '6MOIS'),('OPUS-TRAM7-Mensuel-O ',  '7'),"+
		" ('OPUS-TRAM1-Billet-O ',  '72'),('OPUS-TRAM2-Billet-O ',  '73'),('OPUS-TRAM3-Billet-O ',  '74'),"+
		" ('TRAM1-Abonnement-O ',  '79'),('OPUS-TRAM8-Mensuel-O ',  '8'),('TRAM2-Abonnement-O ',  '80'),"+
		" ('TRAM3-Abonnement-O ',  '81'),('TRAM4-Abonnement-O ',  '82'),('TRAM5-Abonnement-O ',  '83'),"+
		" ('TRAM6-Abonnement-O ',  '84'),('TRAM7-Abonnement-O ',  '85'),('TRAM8-Abonnement-O ',  '86'),"+
		" ('Gratuité8-Mensuel-O ',  '88'),('TRAM1-Abonnement-E ',  '89'),('TRAM2-Abonnement-E ',  '90'),"+
		" ('TRAM3-Abonnement-E ',  '91'),('TRAM4-Abonnement-E ',  '92'),('TRAM5-Abonnement-E ',  '93'),"+
		" ('TRAM6-Abonnement-E ',  '94'),('TRAM7-Abonnement-E ',  '95'),('TRAM8-Abonnement-E ',  '96'),"+
		" ('TRAM1-Abonnement-R ',  '97'),('TRAM2-Abonnement-R ',  '98'),('TRAM3-Abonnement-R ',  '99'),"+
		" ('RTL-TACITE REC ORD. ',  'ABLOR'),('RTL Accès-Gratuité ',  'Accès'),('ADOBUS 7 à€ 17 ANS ',  'ADOBU'),"+
		" ('RTL Ainé-Gratuité ',  'Ainé'),('RTL Accès-Bouchervil ',  'Bouch'),('RTL Accès-Brossard ',  'Bross'),"+
		" ('LP mét MRC CB étu ',  'CBMRE'),('LP mét MRC CB gén ',  'CBMRG'),('LP mét MRC CB 65+ ',  'CBMRN'),"+
		" ('RTL-OPUS-MENS.ORD. ',  'CLO'),('RTL-OPUS-MENS.RÉD. ',  'CLR'),('RTL retr. conjoint ',  'EMPRC'),"+
		" ('Frais administratif ',  'FADM-'),('Frais de privilège ',  'FPRTL'),('RTL-GROUPE ',  'GR15'),"+
		" ('RTL-OPUS-MENS.HRSPO ',  'HRSPO'),('LP mét MRC IO étu ',  'IOMRE'),('LP mét MRC IO gén ',  'IOMRG'),"+
		" ('LP mét MRC IO 65+ ',  'IOMRN'),('LP mét MRC JC étu ',  'JCMRE'),('LP mét MRC JC gén ',  'JCMRG'),"+
		" ('LP mét MRC JC 65+ ',  'JCMRN'),('CPO met MRC CB étu ',  'KBMRE'),('CPO met MRC JC étu ',  'KCMRE'),"+
		" ('L-P metropol étu CPO ',  'KLMRE'),('CPO met MRC IO étu ',  'KOMRE'),('AbonneBus Ainé ',  'LPABA'),"+
		" ('AbonneBus pri ',  'LPABP'),('AbonneBus rég ',  'LPABR'),('LP mensuel Ainé ',  'LPM-A'),"+
		" ('LP mensuel privilège ',  'LPM-P'),('L-P mensuel régulier ',  'LPM-R'),('RTL Employeur ',  'POSTE'),"+
		" ('Abonnebus mét étu ',  'QLARE'),('Abonnebus mét gén ',  'QLARG'),('Abonnebus mét 65+ ',  'QLARN'),"+
		" ('LP métropolitain étu ',  'QLMRE'),('LP métropolitain gén ',  'QLMRG'),('LP métropolitain 65+ ',  'QLMRN'),"+
		" ('Grat8-Abonnement-O ',  'RC164'),('RTL Scolaire 6 mois ',  'SCOL6'),('Scolaire Annuel ',  'SCOLA'), " +
		" ('RTL-MENSUEL-TEMPO ',  'TEMPO'),('RTL-TACITE REC ORD. ',  'TRCLO'),('RTL-TACITE REC RED. ',  'TRCLR'),"+
		" ('RTL-TACITE REC HRP. ',  'TRHRP') ;",
			
	extract_dbo_opus = "CREATE TABLE extract_dbo_opus("+
		"ID_D28_S28	nvarchar(255) not null,"+
		"reseauoperation int null,"+
		"identification	nvarchar(40),"+
		"code_titre	nvarchar(5),"+
		"date24	date,"+
		"date28	date,"+
		"voiture_cap int,"+
		"heure24 time,"+
		"lig_cap int,"+
		"no_bus_cap	varchar(100),"+
		"seconde28	int,"+
		"type_val varchar(100),"+
		"trajet	int,"+
		"ordre_valid int,"+
		"chaine_val	int, "+
		"nb_montants int, "+
		"ordre_valid_total int " +
		")",
		
	extract_gfi="CREATE TABLE extract_gfi("+
		"identification	nvarchar(255),"+
		"reseauoperation int,"+
		"code_titre	char(25),"+
		"assignation varchar(100),"+
		"type_service varchar(2),"+
		"date24	date,"+
		"date28	date NOT NULL,"+
		"heure24 time,"+
		"no_bus_gfi	int,"+
		"ligne_gfi	int,"+
		"voiture_gfi int,"+
		"seconde28	int,"+
		"nb_montants int,"+
		"no_bus	varchar(100),"+
		"voiture varchar(100),"+
		"direction	char(1),"+
		"trace	varchar(100),"+
		"hre_pre_dep varchar(100),"+
		"hre_pre_arr varchar(100),"+
		"pos_course	decimal(7,0),"+
		"source_imput varchar(100),"+
		"PRIMARY KEY (date28, no_bus_gfi, seconde28) with (IGNORE_DUP_KEY = ON)	); "+
		"CREATE INDEX date28 ON extract_gfi(date28 ASC);"+
		"CREATE INDEX identification ON extract_gfi(identification ASC);"+
		"CREATE INDEX no_bus_gfi ON extract_gfi(no_bus_gfi ASC);"+
		"CREATE INDEX seconde28	ON extract_gfi(seconde28 ASC);"+
		"CREATE INDEX voiture_gfi ON extract_gfi(voiture_gfi ASC);",

	extract_ref_arret = " CREATE TABLE extract_ref_arret("+
		"assignation	char(8) Not Null,"+
		"no_arret	numeric(7,0) not null,"+
		"coord_x	numeric(7,0) null,"+
		"coord_y	numeric(7,0) null);"+
		"CREATE INDEX arret ON extract_ref_arret(no_arret ASC);"+
		"CREATE INDEX assig ON extract_ref_arret(assignation ASC); ",

	extract_ref_course = "CREATE TABLE extract_ref_course("+
		"ligne	numeric(3,0) not null,"+
		"assignation	char(8) not null,"+
		"type_service	char(2) not null,"+
		"periode	char(2) null,"+
		"voiture	char(8) not null,"+
		"hre_pre_dep	char(8) not null,"+
		"hre_pre_arr	char (8) null,"+
		"no_seq_course	numeric(2,0) null,"+
		"no_voyage	numeric(2,0) null,"+
		"direction	char(1) null,"+
		"trace	numeric(3,0) null,"+
		"pos_arr	numeric(5,0) null,"+
		"date_j	date ); "+
		"CREATE INDEX hre_pre_dep ON extract_ref_course (hre_pre_dep ASC);"+
		"CREATE INDEX hre_pre_dep1 ON extract_ref_course (hre_pre_arr ASC);"+
		"CREATE INDEX voiture ON extract_ref_course (voiture ASC);",

	extract_ref_course_arret = "CREATE TABLE extract_ref_course_arret("+
		"assignation	char(8),"+
		"type_service	char(2),"+
		"ligne	numeric(3,0),"+
		"voiture char(8),"+
		"direction	char(1),"+
		"hre_pre_dep char(8),"+
		"no_arret	numeric(7,0),"+
		"hre_pre_arret	char(8),"+
		"seconde_hre_pre_dep	int,"+
		"seconde_hre_pre_arret	int,"+
		"tmps_par_pre	int,"+
		"info_acces	varchar(3),"+
		"date_j	date,"+
		"position	numeric(5,0),"+
		"coord_x numeric(7,0),"+
		"coord_y numeric(7,0)); "+
		"CREATE INDEX hre_pre_dep ON extract_ref_course_arret (hre_pre_dep ASC);"+
		"CREATE INDEX seconde_hre_pre_arret ON extract_ref_course_arret (seconde_hre_pre_arret ASC);"+
		"CREATE INDEX seconde_hre_pre_dep ON extract_ref_course_arret (seconde_hre_pre_dep ASC);"+
		"CREATE INDEX voiture ON extract_ref_course_arret (voiture ASC);",

	extract_ref_course_arret_suivant = "CREATE TABLE extract_ref_course_arret_suivant("+
		"assignation	char(8),"+
		"type_service	char(2),"+
		"ligne	numeric (3,0),"+
		"voiture	char(8),"+
		"periode	char(2),"+
		"hre_pre_dep	char(8),"+
		"no_arret	numeric(7,0),"+
		"position	numeric(5,0),"+
		"coord_x	numeric(7,0),"+
		"coord_y	numeric(7,0),"+
		"hre_pre_arr_suivant	char(8),"+
		"direction_suivant	char(1),"+
		"trace_suivant	numeric(3,0),"+
		"ligne_suivant	numeric(3,0),"+
		"hre_pre_dep_suivant	char(8),"+
		"no_arret_suivant	numeric(7,0),"+
		"position_suivant	numeric(5,0),"+
		"coord_x_suivant	numeric(7,0),"+
		"coord_y_suivant	numeric(7,0)); "+
		"CREATE INDEX arret ON extract_ref_course_arret_suivant (no_arret ASC);"+
		"CREATE INDEX hre_pre_dep ON extract_ref_course_arret_suivant (hre_pre_dep ASC);"+
		"CREATE INDEX ligne ON extract_ref_course_arret_suivant (ligne ASC);"+
		"CREATE INDEX voiture ON extract_ref_course_arret_suivant (voiture ASC);",

	extract_sar_bus =	"CREATE TABLE extract_sar_bus("+
		"date_j	date,"+
		"voiture	char(8),"+
		"hre_dep	char(8),"+
		"hre_fin	char(8),"+
		"no_bus	char(5),"+
		"sec_deb_voiture	Float,"+
		"sec_fin_voiture	Float,"+
		"assignation	char(8),"+
		"type_service	char(2),"+
		"seq_bus int);", 

	extract_sdap_course ="CREATE TABLE extract_sdap_course("+
		"date_j	date,"+
		"no_bus	varchar(100),"+
		"hre_pre_dep	varchar(100),"+
		"voiture	varchar(100),"+
		"sec_dep_course_reel	Float,"+
		"sec_arr_course_reel	Float);",

	extract_sdap_course_arret=	"CREATE TABLE extract_sdap_course_arret("+
		"assignation	char(8),"+
		"date_j	date,"+
		"voiture	char(8),"+
		"seconde_arr_arret	int,"+
		"seconde_dep_arret	int,"+
		"no_arret	numeric(7,0),"+
		"rue_inter	varchar(100),"+
		"hre_pre_dep char(8),"+
		"position	numeric(5,0),"+
		"coord_x	numeric(7,0),"+
		"coord_y	numeric(7,0),"+
		"PRIMARY KEY (date_j, voiture, seconde_arr_arret, position) with (IGNORE_DUP_KEY = ON) ); "+
		"CREATE INDEX date_j ON extract_sdap_course_arret (date_j ASC);"+
		"CREATE INDEX hre_pre_dep ON extract_sdap_course_arret (hre_pre_dep ASC);"+
		"CREATE INDEX seconde_arr_arret ON extract_sdap_course_arret (seconde_arr_arret ASC);"+
		"CREATE INDEX seconde_dep_arret ON extract_sdap_course_arret (seconde_dep_arret ASC);",

	liste_arret_boucle="CREATE TABLE liste_arret_boucle("+
		"date_j	date,"+
		"assignation	varchar(100),"+
		"type_service	char(2),"+
		"voiture	varchar(100),"+
		"ligne	decimal,"+
		"hre_pre_dep	varchar(100),"+
		"dep_suivant_boucle	varchar(100),"+
		"direction	char(1),"+
		"trace	varchar(100),"+
		"position_boucle	decimal,"+
		"coord_x decimal,"+
		"coord_y decimal,"+
		"seconde_hre_pre_arret	int,"+
		"seconde_hre_reel_arret	int,"+
		"no_arret decimal,"+
		"reseauoperation int); "+
		"CREATE INDEX hre_pre_dep ON liste_arret_boucle (hre_pre_dep ASC);"+
		"CREATE INDEX hre_pre_dep1 ON liste_arret_boucle (dep_suivant_boucle ASC);"+
		"CREATE INDEX no_arret ON liste_arret_boucle (no_arret ASC);"+
		"CREATE INDEX pos ON liste_arret_boucle (position_boucle ASC);"+
		"CREATE INDEX seconde_pre ON liste_arret_boucle (seconde_hre_pre_arret ASC);"+
		"CREATE INDEX seconde_reel ON liste_arret_boucle (seconde_hre_reel_arret ASC);"+
		"CREATE INDEX voiture ON liste_arret_boucle (voiture ASC);",

	ordonnancement_Valid="CREATE TABLE ordonnancement_Valid("+
		"ID_D28_S28	nvarchar(255) not null, "+
		"identification	varchar(100),"+
		"date28	date,"+
		"seconde28 int,"+
		"ordre_valid int);",

	pos_course_dans_voiture="CREATE TABLE pos_course_dans_voiture("+
		"date_j	date,"+
		"type_service char(2),"+
		"voiture varchar(100),"+
		"pre_course	decimal,"+
		"der_course	decimal,"+
		"no_bus	varchar(100),"+
		"assignation char(8),"+
		"hre_pre_dep_pre varchar(100),"+
		"hre_pre_arr_der varchar(100)"+
		"PRIMARY KEY (date_j, type_service, voiture, no_bus) with(IGNORE_DUP_KEY = ON) );", 

	semaine_relache="CREATE TABLE semaine_relache(jour_relache date not null PRIMARY KEY);"+
		" INSERT INTO semaine_relache(jour_relache) values ('2014-03-03'),('2014-03-04'),('2014-03-05'),('2014-03-06'),('2014-03-07'),('2015-03-02'), "+
		" ('2015-03-03'),('2015-03-04'),('2015-03-05'),('2015-03-06'), ('2016-02-29'),('2016-03-01'),('2016-03-02'),('2016-03-03'),('2016-03-04'), "+
		" ('2017-02-27'),('2017-02-28'),('2017-03-01'),('2017-03-02'),('2017-03-03'),('2018-03-05'),('2018-03-06'),('2018-03-07'),('2018-03-08'),('2018-03-09'), "+
		" ('2019-03-04'),('2019-03-05'),('2019-03-06'),('2019-03-07'),('2019-03-08'); ",

	validations_successives="CREATE TABLE validations_successives("+
		"ID_D28_S28	nvarchar(255),"+
		"identification	varchar(100),"+
		"date28	date,"+
		"voiture varchar(100),"+
		"hre_pre_dep_1	varchar(100),"+
		"seconde_1	int,"+
		"no_arret_emb_1	decimal,"+
		"position_1	decimal,"+
		"seconde_2	int,"+
		"no_arret_emb_2	decimal,"+
		"coord_x_emb_2	decimal,"+
		"coord_y_emb_2	decimal	); "+
		"CREATE INDEX hre_pre_dep ON validations_successives(hre_pre_dep_1 ASC); "+
		"CREATE INDEX position_1 ON validations_successives(position_1 ASC); "+
		"CREATE INDEX seconde_2 ON validations_successives(seconde_2 ASC); "+
		"CREATE INDEX voiture ON validations_successives(voiture ASC); ";
	
	//Liste des tables à créer -- définition
	let tablist = [ CAP_GFI_temp, calendrier, bus_course, boucle_course, BD_Valid_Metro, arrets_probables, arrets_possibles, arret_metro, 
					extract_dbo_opus, dico_titre, dernieres_validations, chaine_validation, chaine_validation2, chaine_validation3, 
					extract_ref_arret, extract_ref_course, extract_sar_bus, extract_ref_course_arret , extract_ref_course_arret_suivant,
					extract_sdap_course, extract_sdap_course_arret,liste_arret_boucle,ordonnancement_Valid, 
					pos_course_dans_voiture, semaine_relache, validations_successives ];

	//Liste des tables créées -- array of table names
	let tabname = [ "CAP_GFI_temp", "calendrier", "bus_course", "boucle_course", "BD_Valid_Metro", "arrets_probables", "arrets_possibles", 
					"arret_metro", "extract_dbo_opus", "dico_titre", "dernieres_validations", "chaine_validation", "chaine_validation2", 
					"chaine_validation3", "extract_ref_arret", "extract_ref_course", "extract_sar_bus", "extract_ref_course_arret", 
					"extract_ref_course_arret_suivant", "extract_sdap_course", "extract_sdap_course_arret", "liste_arret_boucle", 
					"ordonnancement_Valid", "pos_course_dans_voiture", "semaine_relache", "validations_successives" ];
	
	let i=0, j=0, request = new sql.Request();
	essai++;  //nb de fois le processus a essayé de créer les tables
	
	console.log("Requete lancée");
	for(let k=0; k<tablist.length; ++k){
		request.query("IF OBJECT_ID('"+tabname[k]+"', 'U') IS NOT NULL DROP TABLE "+tabname[k]+"; "+tablist[k], function (err, getdata) {
			if (err) {
				console.log('\n #'+j++ +'\n'+tablist[k]+'\n'+err); 
				if(essai==3) COMPILATION_OPUS_GFI(); // on va re-essayer d'effacer les tables une seule fois
				else setTimeout( ()=>{lastDate()}, 1000*60*1); //reinialiser dans 1 min le programme
			}
			else{ 
				console.log (i +' -- '+ ftimestamp(new Date())+' Table '+ tabname[k]+' -- créée avec succès');
				i++;
				if(i==tablist.length) {
					console.log ('\nPousser les requetes SQL extraction de données vers les tables temporaires');
					sql.close();
					Requetes_SQL();
				}
			}
		});
	}
}

// Cette fonction effacera la liste de tables 
// si elles existent avant de lancer la fonction création de tables
function deleteTable(tab2del){  
	console.log('\nPromise 6.2 -- '+ftimestamp(new Date())+' Suppression des tables temporaires');
	let i=0, j=0;
	let request = new sql.Request();
	for(let k=0; k<=tab2del.length; ++k){
		request.query("IF OBJECT_ID('"+tab2del[k]+"', 'U') IS NOT NULL DROP TABLE "+tab2del[k], function (err, getdata) {
			
			if(k>=tab2del.length){ 
				setTimeout( ()=>tableCreation(), 15000)
			}	
			else{ 
				if (err) {
					console.log('\n #'+j++ +'\n'+tab2del[k]+'\n'+err); 
				}
				else{
					console.log (k +' -- '+ ftimestamp(new Date())+' - Table '+ tab2del[k]+ '-- supprimée avec succès');
					i++;
				}
			}	
		});
	}
};

// Cette fonction est non chainée, elle effacera tout simplement la "liste" de tables
// passée en paramètres
function delTable(tab2){  
	console.log('\nPromise 6.2 -- '+ftimestamp(new Date())+' Suppression des tables temporaires');
	let i=0, j=0;
	let request = new sql.Request();
	for(let k=0; k<=tab2.length; ++k){
		request.query("IF OBJECT_ID('"+tab2[k]+"', 'U') IS NOT NULL DROP TABLE "+tab2[k], function (err, getdata) {
			if (err) {
				console.log('\n #'+j++ +'\n'+tab2[k]+'\n'+err); 
			}else{
			}
		});
	}
};

// Cette fonction générer les dates et les mettre au bon format 
// pour les requêtes SQL
function genDate(){
	console.log('#3a - Generer les dates');
	console.log('#3a - Dernier Jour Traite: ', global.DernierJourTraite);
	global.debutTraitement = new Date();  //timestamp
	// Si pas de données dans la BDD 

	//ANNÉE DU TRAITEMENT
	global.ANNEEopus4 = new Date((+global.DernierJourTraite) + 86400000*1).getFullYear();  //AAAA
	global.ANNEEopus2 = global.ANNEEopus4-2000;  //AA
	
 	//si premier janvier est le dernier jour traité, passer à année+1 puis changer de BDD
	let mm=new Date((+global.DernierJourTraite) + 86400000*1).getMonth+1, jj=new Date((+global.DernierJourTraite) + 86400000*1).getDate();
	if(mm==1 && jj==1) {
		global.ANNEEopus4= new Date((+global.DernierJourTraite) + 86400000*1).getFullYear()+1;
		global.ANNEEopus2 = global.ANNEEopus4-2000;
	} 

	//Période à traiter	
	global.dateDebut= formatdate(new Date((+global.DernierJourTraite) + 86400000*1)); // debut période à traiter  aaaa-mm-jj = dernier jour traité
	global.dateFin= formatdate(new Date((+global.DernierJourTraite) + 86400000*2)); // fin période à traiter  aaaa-mm-jj  --  on traite une journée à la fois par défaut

	global.DATETxtDebut = "'"+formatdate01(new Date((+global.DernierJourTraite) + 86400000*1))+"'"; // 'aaaammjj'
	global.DATETxtFin = "'"+formatdate01(new Date((+global.DernierJourTraite) + 86400000*2))+"'"; // 'aaaammjj'
	
	global.DATEDebut = formatdate01(new Date((+global.DernierJourTraite) + 86400000*1)); // aaaammjj
	global.DATEFin = formatdate01(new Date((+global.DernierJourTraite) + 86400000*2));  // aaaammjj
	
	global.DTHRDebut = formatdate(new Date((+global.DernierJourTraite) + 86400000*1)) + " 04:00:00"; // AAAA-MM-JJ HH:MM:SS
	global.DTHRFin = formatdate(new Date((+global.DernierJourTraite) + 86400000*2)) + " 03:59:59"; // AAAA-MM-JJ HH:MM
	
	global.period=global.DATETxtDebut+" AND "+global.DATETxtDebut; //periode pour sql query
	console.log('periode '+global.dateDebut +' - '+ global.dateFin);
	console.log('\n');
	console.log('DateDebut : ', global.dateDebut);
	console.log('DateFin : ', global.dateFin);
	console.log('\n');
	console.log('DATEDebut : ', global.DATEDebut);
	console.log('DATEFin : ', global.DATEFin);
	console.log('\n');
	
	console.log('DATETxtDebut : ', global.DATETxtDebut);
	console.log('DATETxtFin: ', global.DATETxtFin);
	console.log('\n');
	
	console.log('DTHRDebut ', global.DTHRDebut);
	console.log('DTHRFin ', global.DTHRFin);
	console.log('\n');
	
	console.log('ANNEEopus2 ', global.ANNEEopus2);
	console.log('ANNEEopus4 ', global.ANNEEopus4);
	console.log('\n');
	return  new Promise( function(resolve, reject){
		setTimeout ( function(){
			console.log("\nLa requête Générer les dates est complétée\n");
			resolve(Jour_de_la_semaine());
		},5000)
	})
}

//'RQ pour nettoyage des tables temporaires
var Vidange_table_temporaire_1 = "TRUNCATE TABLE pos_course_dans_voiture";
var Vidange_table_temporaire_2 = "TRUNCATE TABLE CAP_GFI_temp";//CAP_GFI
var Vidange_table_temporaire_3 = "TRUNCATE TABLE bus_course";
var Vidange_table_temporaire_4 = "TRUNCATE TABLE extract_dbo_opus";
var Vidange_table_temporaire_5 = "TRUNCATE TABLE ordonnancement_Valid";
var Vidange_table_temporaire_6 = "TRUNCATE TABLE chaine_validation";
var Vidange_table_temporaire_7 = "TRUNCATE TABLE extract_SDAP_course_arret";
var Vidange_table_temporaire_8 = "TRUNCATE TABLE extract_ref_course_arret";
var Vidange_table_temporaire_9 = "TRUNCATE TABLE extract_ref_course_arret_suivant";
var Vidange_table_temporaire_10 = "TRUNCATE TABLE extract_gfi";
var Vidange_table_temporaire_11 = "TRUNCATE TABLE extract_ref_course";
var Vidange_table_temporaire_12 = "TRUNCATE TABLE boucle_course";
var Vidange_table_temporaire_13 = "TRUNCATE TABLE liste_arret_boucle";
var Vidange_table_temporaire_14 = "TRUNCATE TABLE extract_ref_arret";
var Vidange_table_temporaire_15 = "TRUNCATE TABLE BD_Valid_Metro";
var Vidange_table_temporaire_16 = "TRUNCATE TABLE validations_successives";
var Vidange_table_temporaire_17 = "TRUNCATE TABLE arrets_possibles";
var Vidange_table_temporaire_18 = "TRUNCATE TABLE arrets_probables";
var Vidange_table_temporaire_19 = "TRUNCATE TABLE dernieres_validations";
var Vidange_table_temporaire_20 = "TRUNCATE TABLE chaine_validation2";
var Vidange_table_temporaire_21 = "TRUNCATE TABLE chaine_validation3";
var Vidange_table_temporaire_22 = "TRUNCATE TABLE extract_sdap_course";
var Vidange_table_temporaire_23 = "TRUNCATE TABLE extract_sar_bus";
var Vidange_table_temporaire_24 = "TRUNCATE TABLE ordonnancement_Valid_2";
	
//'RQ paramètre de base : mise à jour du calendrier (type de service, assignation) 
var Alimentation_Calendrier = "UPDATE a "+
	" SET a.assignation = c.assignation, "+
	" a.annee = year(a.date_j), a.mois = month(a.date_j), a.jour = day(a.date_j),  "+
	" a.type_service = IIf(LEFT(b.type_service,1) = 'F','FE',IIf(DATEPART(dw, a.date_j)=7,'SA',IIf(DATEPART(dw, a.date_j)=1,'DI','SE'))),  "+
	" a.type_service2 = IIf(LEFT(b.type_service,1) = 'F',b.type_service, IIf(DATEPART(dw, a.date_j)=7,'SA',IIf(DATEPART(dw, a.date_j)=1,'DI','SE'))),  "+ //attention to localization
	" a.relache_scolaire = IIf(SUBSTRING(c.assignation,6,1) In (6,7),-1,IIf(d.jour_relache Is Not Null,-1,0)) , "+
	" a.date_num = SUBSTRING(CAST( a.date_j AS VARCHAR(255)),1,4)+SUBSTRING(CAST(a.date_j AS VARCHAR(255)),6,2)+SUBSTRING(CAST(a.date_j AS VARCHAR(255)),9,2) "+
	" FROM calendrier as a "+
	" LEFT JOIN Stad.stad.ref_calendrier AS b ON SUBSTRING(CAST( a.date_j AS VARCHAR(255)),1,4)+SUBSTRING(CAST(a.date_j AS VARCHAR(255)),6,2)+SUBSTRING(CAST(a.date_j AS VARCHAR(255)),9,2) =b.date_ferie "+
	" LEFT JOIN Stad.stad.ref_assignation AS c ON SUBSTRING(CAST(a.date_j AS VARCHAR(255)),1,4)+SUBSTRING(CAST(a.date_j AS VARCHAR(255)),6,2)+SUBSTRING(CAST(a.date_j "+
	" AS VARCHAR(255)),9,2)<=c.date_fin AND (SUBSTRING(CAST(a.date_j AS VARCHAR(255)),1,4)+SUBSTRING(CAST(a.date_j AS VARCHAR(255)),6,2)+SUBSTRING(CAST(a.date_j "+
 	" AS VARCHAR(255)),9,2)>=c.assignation) LEFT JOIN dbo.semaine_relache AS d ON a.date_j=d.jour_relache; ";

//'RQ nécessaires à l'élaboration de la table bus_course 	
 var Alimentation_bus_course =" INSERT INTO AchalDep.dbo.bus_course SELECT "+
	" null as No, convert(date, b.date_j) AS date_j, null as no_bus, null as no_bus_sdap, LTRIM(RTRIM(b.voiture)) AS voiture, "+
	" b.assignation AS assignation, b.type_service AS type_service, a.ligne AS ligne, null as hre_deb_voiture_veh, "+
	" null as hre_fin_voiture_veh, a.hre_pre_dep AS hre_pre_dep, a.hre_pre_arr AS hre_pre_arr, a.direction AS direction,  "+
	" a.trace AS trace, a.periode AS periode, SUBSTRING(a.hre_pre_dep,1,2)*3600+SUBSTRING(a.hre_pre_dep,4,2)*60+SUBSTRING(a.hre_pre_dep,7,2) AS sec_dep_course_pre, "+
	" SUBSTRING(a.hre_pre_arr,1,2)*3600+SUBSTRING(a.hre_pre_arr,4,2)*60+SUBSTRING(a.hre_pre_arr,7,2) AS sec_arr_course_pre, "+
	" null as sec_dep_course_reel, null as sec_arr_course_reel, null as sec_deb_voiture, null as sec_fin_voiture, " +
	" IIF(a.no_seq_course=b.pre_course, '1', IIF(a.no_seq_course=b.der_course,'2','0')) AS pos_course "+
	" FROM [Stad].[stad].[ref_course] AS a INNER JOIN pos_course_dans_voiture AS b ON a.assignation=b.assignation "+
	" AND a.voiture=b.voiture AND a.type_service=b.type_service AND a.hre_pre_dep>= b.hre_pre_dep_pre AND a.hre_pre_arr<= b.hre_pre_arr_der "+
	" WHERE a.ligne <> 0; "; 

// var Extraction_sar_bus 
var Extraction_sar_bus ;
	
//'la jointure avec sdap_course_arret sert à intégrer les montées faites au premier arrêt avant le début réel de la course -- OK
var Extraction_sdap_course;

//VAR  Update_bus_course_avec_extract_sar_bus 
var correctionWDK = " select *, seq_bus=row_number() over (partition by voiture, ligne,hre_pre_dep, hre_pre_arr, direction, sec_dep_course_pre, sec_arr_course_pre "+
  " order by voiture, ligne,hre_pre_dep, hre_pre_arr, direction, sec_dep_course_pre, sec_arr_course_pre )"+
  " into bus_course_ from bus_course; drop table bus_course;";

var Update_bus_course_avec_extract_sar_bus = " select * into bus_course from bus_course_; UPDATE BUS_COURSE "+
	" SET BUS_COURSE.hre_deb_voiture_veh = b.hre_dep, BUS_COURSE.hre_fin_voiture_veh = b.hre_fin, BUS_COURSE.sec_deb_voiture = b.sec_deb_voiture, "+
	" BUS_COURSE.sec_fin_voiture = b.sec_fin_voiture, BUS_COURSE.no_bus = b.no_bus FROM BUS_COURSE a "+
	" LEFT JOIN extract_sar_bus AS b ON a.date_j=b.date_j AND a.voiture=b.voiture AND a.hre_pre_dep>=b.hre_dep AND "+
	" a.hre_pre_arr<=b.hre_fin AND a.seq_bus = b.seq_bus; drop table bus_course_; "; //ajouter where seq_bus = seq_bus

//var Update_bus_course_avec_extract_sdap_course
var Update_bus_course_avec_extract_sdap_course = "UPDATE BUS_COURSE "+ 
	" SET BUS_COURSE.sec_dep_course_reel = b.sec_dep_course_reel, BUS_COURSE.sec_arr_course_reel = b.sec_arr_course_reel, BUS_COURSE.no_bus_sdap = b.no_bus "+
	" FROM BUS_COURSE AS a "+
	" INNER JOIN extract_sdap_course AS b ON a.date_j=b.date_j AND a.voiture=b.voiture AND a.hre_pre_dep = b.hre_pre_dep; ";

//var Position_course_dans_voiture 
var Position_course_dans_voiture = "INSERT INTO pos_course_dans_voiture "+
	" SELECT b.date_j as date_j, a.assignation AS assignation, a.type_service AS type_service, a.voiture AS voiture, b.no_bus as no_bus, MIN(a.no_seq_course) AS pre_course, "+
	" MAX(a.no_seq_course) AS der_course, "+
	" MIN(a.hre_pre_dep) AS hre_pre_dep_pre ,MAX(a.hre_pre_arr) AS hre_pre_arr_der "+
	" FROM extract_ref_course AS a "+
	" LEFT JOIN extract_sar_bus as b ON a.voiture=b.voiture AND a.hre_pre_dep>=b.hre_dep AND a.hre_pre_arr<=b.hre_fin "+
	" WHERE b.date_j BETWEEN '"+ global.dateDebut+ "' AND ' "+ global.dateFin +"'"+
	" GROUP BY b.date_j, a.assignation, a.type_service, a.voiture, b.no_bus; ";
	
//'RQ Extraction et prétraitement de la donnée gfi

//'principe de conversion de l'argent perçu en achalandage :
/*  '- les sommes inférieures à 1.65 $ sont évacuées
    '- les sommes inférieures à 3.25x + 1.65 sont arrondies au nb_montants inférieur
    '- les sommes supérieures ou égales à 3.25x + 1.65 sont arrondies au nb_montants supérieur
*/    
var Extraction_dbo_Opus, Extraction_gfi;
//'Union des tables gfi et opus
var Reunion_extract_opus_gfi = " INSERT INTO extract_dbo_opus "+
	" SELECT '' as ID_D28_S28, reseauoperation,identification, convert(nvarchar,code_titre) as code_titre, date24, date28, "+
	" voiture_gfi as voiture_cap, heure24, ligne_gfi as lig_cap, cast(no_bus_gfi as int) as no_bus_cap, seconde28,  "+
	" null as type_val, 0 as trajet, 0 as ordre_valid, null as chaine_val,  "+   
	" nb_montants, 0 as ordre_valid_total  "+
	" from extract_gfi;  ";
		
var Cle_ID = "UPDATE extract_dbo_opus "+
	" SET ID_D28_S28 = convert(nvarchar(255), CONCAT(identification,'_', date28, '_', seconde28) );"; 

//'RQ nécessaires à la numérotation DES VALIDATIONS DE CHAQUE USAGERS DANS L'ORDRE CHRONOLOGIQUE
var Numerotation_toutes_valid = 
	" INSERT INTO ordonnancement_Valid "+
	" SELECT a.ID_D28_S28 as ID_D28_S28, a.identification as identification, a.date28 as date28, a.seconde28 as seconde28, COUNT(b.seconde28) AS ordre_valid"+
	" FROM extract_dbo_opus as a "+
	" LEFT OUTER JOIN extract_dbo_opus as b ON a.identification=b.identification AND a.date28=b.date28 AND a.seconde28 >= b.seconde28  "+
	" GROUP BY a.ID_D28_S28, a.seconde28, a.identification, a.date28; ";
	
var Update_Numerotation_toutes_valid = " Update extract_dbo_opus "+ 
	" SET extract_dbo_opus.ordre_valid_total=b.ordre_valid "+
	" FROM extract_dbo_opus As a INNER Join ordonnancement_Valid as b ON a.ID_D28_S28=b.ID_D28_S28 ; "+
	//'Alteration de la table ordonnançant les validations de chaque usager
	" Use achaldep; Alter table ordonnancement_Valid "+
	" ADD type_val varchar(100),trajet int, lig_cap decimal; "+
	" CREATE INDEX id_date28_lig ON ordonnancement_Valid (identification ASC, date28 ASC, lig_cap ASC);"+
	" CREATE INDEX id_date28_lig_cap_trajet ON ordonnancement_Valid (identification ASC, date28 ASC, seconde28 ASC, lig_cap ASC, trajet ASC);"+
	" CREATE INDEX ordre_valid ON ordonnancement_Valid (ordre_valid ASC); ";

//'----------- ************************** ----------------------                                                    
//'RQ nécessaires à l'élaboration de la table CAP_GFI et la détermination du type de validation : 
//1ère montée ou correspondance (sur le Réseau de Transport de Longueuil uniquement)
 
//'Alimentation de la table ordonnançant les validations de chaque usagers
var Alimentation_Ordo_Valid = " TRUNCATE TABLE ordonnancement_Valid;"+
	" INSERT INTO ordonnancement_Valid "+
	" SELECT a.ID_D28_S28 as ID_D28_S28, a.identification as identification, a.date28 as date28, a.seconde28 as seconde28, COUNT(b.seconde28) AS ordre_valid, a.type_val as type_val, a.trajet as trajet, "+
	" a.lig_cap as lig_cap FROM extract_dbo_opus as a "+
	" LEFT OUTER JOIN extract_dbo_opus as b ON a.identification=b.identification AND a.date28=b.date28 AND a.seconde28 >= b.seconde28 "+
	" WHERE b.trajet=0  and a.reseauoperation=33 and b.reseauoperation=33 "+
	" GROUP BY a.ID_D28_S28, a.seconde28, a.identification,a.date28, a.lig_cap, a.trajet, a.type_val;"; 

//'Attributions des caractéristiques type_val=Bj1 et trajet=1 à la 1ère validation de la journée de chaque usagers
var Attribution_1ere_valid = " UPDATE ordonnancement_Valid SET trajet=1,type_val='Bj1' WHERE ordre_valid=1 ; ";

//'Alimentation de la table indiquant la numérotation du dernier trajet courant
var Alimentation_ordo_Valid_2; 

//'Ajout des Bjc respectant les règles (90min, ligne différente) par rapport au Bj1
var Update_ordo_Valid_1 ; 

//'Correction des Bjc en Bj1 si un autre Bjc du même trajet a déjà été effectué sur la même ligne
var Update_ordo_Valid_2 ;

//'Attribution de Bj1 à toutes les validations encore non atribuées de l'ordre_validation en cours de traitement
var Update_ordo_Valid_3 ; 

//'Intégration des types de validation et trajet à extract_dbo_opus
var Update_extract_dbo_Valid_avec_correspondance = " UPDATE extract_dbo_opus "+
" SET extract_dbo_opus.trajet = b.trajet , extract_dbo_opus.type_val=b.type_val , extract_dbo_opus.ordre_valid=b.ordre_valid "+
" FROM extract_dbo_opus As a INNER Join ordonnancement_Valid As b ON a.ID_D28_S28=b.ID_D28_S28 ";
	
//'Table des validations sur le réseau RTL	
var Alimentation_CAP_GFI = 
" INSERT INTO CAP_GFI_temp SELECT "+
" convert(nvarchar(255), CONCAT(a.identification,'_', a.date28, '_', a.seconde28) ) AS ID_D28_S28, "+
" a.identification, a.code_titre, c.titre_nom AS nom_titre, a.date24, a.date28,a.heure24, a.seconde28, "+
" cast(cast(concat(a.date24, ' ', a.heure24) as datetime2) as datetime) AS Date_complet_28, d.assignation as assignation,d.type_service2 AS type_service,a.no_bus_CAP, "+
" null as no_bus,a.voiture_CAP,null as voiture,a.lig_cap,null as ligne,null as periode, null as hre_pre_dep, null as hre_pre_arr, "+
" null as direction,null as trace,null as pos_course, a.type_val,a.ordre_valid, a.trajet, a.ordre_valid_total,  "+  
" a.chaine_val, a.nb_montants,null as no_arret_emb,null as no_arret_deb,null as position_arret_emb, "+
" null as coord_x_emb,null as coord_y_emb,null as source_imput,null as imput_emb,null as imput_deb "+
" FROM extract_dbo_opus AS a LEFT JOIN dico_titre AS c ON a.code_titre = c.Prd_Numero LEFT JOIN calendrier AS d ON a.date28=d.date_j WHERE a.reseauoperation=33 "+
" EXCEPT SELECT * FROM cap_gfi_temp; "; //exclure insertion de doublon
		
//Table des validations sur le métro (en correspondance depuis le réseau RTL)
 var Alimentation_BD_VALID_METRO = 
" INSERT INTO BD_Valid_Metro "+
" SELECT a.ID_D28_S28, a.identification, a.date28, a.seconde28, a.lig_cap AS ligne, cast(a.no_bus_cap as int) AS station, a.ordre_valid_total, b.coord_x, b.coord_y  "+
" FROM extract_dbo_opus as a  "+
" INNER JOIN arret_metro as b ON cast(a.no_bus_cap as int) = b.no_station "+ 
" WHERE a.ordre_valid_total>1;	";


//'RQ DE MATCHS ENTRE VALIDATIONS OPUS ET DONNÉES COURSES
var MATCH1 = " UPDATE CAP_GFI_temp SET"+
" CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = b.no_bus, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'A10', "+
" CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, "+
" CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.voiture_cap=b.voiture AND a.seconde28 <=b.sec_arr_course_reel "+
" AND a.seconde28 >= b.sec_dep_course_reel AND cast(a.no_bus_cap as int)=b.no_bus_sdap AND a.lig_cap = b.ligne AND a.date28 = b.date_j "+  
" WHERE a.source_imput Is Null;";

var MATCH2 = "UPDATE CAP_GFI_temp SET "+
" CAP_GFI_temp.periode=b.periode,CAP_GFI_temp.no_bus = b.no_bus, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'A20', "+
" CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, "+
" CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.seconde28 <=b.sec_arr_course_reel AND a.seconde28 >= b.sec_dep_course_reel "+
" AND cast(a.no_bus_cap as int)=b.no_bus_sdap AND a.lig_cap = b.ligne AND a.date28 = b.date_j "+
" WHERE a.source_imput is Null;";
	
var MATCH3 = " UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = b.no_bus, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'A30', "+
" CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, "+
" CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.seconde28 <=b.sec_arr_course_reel AND a.seconde28 >= b.sec_dep_course_reel AND a.no_bus_CAP=b.no_bus_sdap AND "+
" a.date28 = b.date_j WHERE a.source_imput is Null;";
	
var MATCH4 = " UPDATE CAP_GFI_temp SET "+ 
" CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = cast(a.no_bus_cap as int), CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, "+
" CAP_GFI_temp.source_imput = 'A40', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, "+
" CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.voiture_cap=b.voiture AND "+
" a.seconde28 >= b.sec_dep_course_reel AND a.seconde28 <=b.sec_arr_course_reel "+
" WHERE a.source_imput is Null;";

var MATCH5 = " UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = b.no_bus, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, "+
" CAP_GFI_temp.source_imput = 'A11', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, "+
" CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.voiture_cap=b.voiture AND a.seconde28 <=b.sec_arr_course_reel AND a.seconde28 >= b.sec_dep_course_reel-300 "+
" AND a.no_bus_CAP=b.no_bus_sdap AND a.lig_cap = b.ligne AND a.date28 = b.date_j "+
" WHERE a.source_imput is Null;";
	
var MATCH6 = " UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = b.no_bus,CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'A21', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.no_bus_CAP=b.no_bus_sdap AND a.seconde28 >= b.sec_dep_course_reel-300 AND a.seconde28 <=b.sec_arr_course_reel "+
" WHERE a.source_imput is Null;";

var MATCH7 = "UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = b.no_bus, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'A31', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.no_bus_CAP=b.no_bus_sdap AND a.seconde28 >= b.sec_dep_course_reel-300 AND a.seconde28 <=b.sec_arr_course_reel WHERE a.source_imput is Null;";

var MATCH8 =  "UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = no_bus_cap, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'A41', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.voiture_cap=b.voiture AND a.seconde28 >= b.sec_dep_course_reel-300 AND a.seconde28 <=b.sec_arr_course_reel WHERE a.source_imput is Null;";
	
var MATCH9= "UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = b.no_bus, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'B10', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.no_bus_CAP=b.no_bus AND a.seconde28 >= b.sec_dep_course_pre AND a.seconde28 <=b.sec_arr_course_pre AND a.voiture_cap=b.voiture "+
" WHERE a.source_imput is Null;";
	
var MATCH10 = "UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = b.no_bus, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'B20', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.no_bus_CAP=b.no_bus AND a.seconde28 >= b.sec_dep_course_pre AND a.seconde28 <=b.sec_arr_course_pre "+
" WHERE a.source_imput is Null;";
	
var MATCH11 = "UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = b.no_bus, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'B30', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction,CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.no_bus_CAP=b.no_bus AND a.seconde28 >= b.sec_dep_course_pre AND a.seconde28 <=b.sec_arr_course_pre "+
" WHERE a.source_imput is Null;";
	
var MATCH12 = "UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus =a.no_bus_cap, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'B40', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace,CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.voiture_cap=b.voiture AND a.seconde28 >= b.sec_dep_course_pre AND a.seconde28 <=b.sec_arr_course_pre WHERE a.source_imput is Null;";

var MATCH13 = "UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = b.no_bus, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'B11', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.no_bus_CAP=b.no_bus AND a.seconde28 >= b.sec_dep_course_pre-300 AND a.seconde28 <=b.sec_arr_course_pre AND a.voiture_cap=b.voiture "+
" WHERE a.source_imput is Null;"; 
		
var MATCH14 = "UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = b.no_bus, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'B21',CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.no_bus_CAP=b.no_bus AND a.seconde28 >= b.sec_dep_course_pre-300 AND a.seconde28 <=b.sec_arr_course_pre  "+
" WHERE a.source_imput is Null;";
	
var MATCH15 = "UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = b.no_bus, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne,CAP_GFI_temp.source_imput = 'B31', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON (a.date28 = b.date_j) AND (a.no_bus_CAP=b.no_bus) AND (a.seconde28 >= b.sec_dep_course_pre-300) AND (a.seconde28 <=b.sec_arr_course_pre) WHERE a.source_imput is Null;";
	
var MATCH16 = "UPDATE CAP_GFI_temp " +
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = no_bus_cap, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'B41', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr,CAP_GFI_temp.direction = b.direction,CAP_GFI_temp.trace = b.trace,CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON (a.date28 = b.date_j) AND (a.lig_cap = b.ligne) AND (a.voiture_cap=b.voiture) AND (a.seconde28 >= b.sec_dep_course_pre-300) AND (a.seconde28 <=b.sec_arr_course_pre) WHERE a.source_imput is Null;";

var MATCH17 = " UPDATE a "+
" SET a.periode=b.periode,a.no_bus = b.no_bus, a.voiture = b.voiture, a.ligne = b.ligne, a.source_imput = 'C11', a.hre_pre_dep = b.hre_pre_dep, a.hre_pre_arr = b.hre_pre_arr, a.direction = b.direction, a.trace = b.trace, a.pos_course = b.pos_course "+
" FROM CAP_GFI_temp AS a LEFT JOIN CAP_GFI_temp AS b ON (a.date28 = b.date28) AND (a.lig_cap = b.lig_cap) AND (a.no_bus_cap = b.no_bus_cap) AND (a.voiture_cap = b.voiture_cap) AND a.seconde28<= SUBSTRING(b.hre_pre_dep,1,2)*3600 + SUBSTRING(b.hre_pre_dep,4,2)*60 + SUBSTRING(b.hre_pre_dep,7,2) AND a.seconde28> SUBSTRING(b.hre_pre_dep,1,2)*3600 + SUBSTRING(b.hre_pre_dep,4,2)*60 + SUBSTRING(b.hre_pre_dep,7,2) - 1200 "+
" WHERE a.source_imput is Null AND  b.source_imput is Not Null  AND b.pos_course = 1; ";
	
var MATCH18 = "UPDATE a "+
" SET periode=b.periode, no_bus = b.no_bus, voiture = b.voiture, ligne = b.ligne, source_imput = 'C21', hre_pre_dep = b.hre_pre_dep, hre_pre_arr = b.hre_pre_arr, direction = b.direction, trace = b.trace, pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a LEFT JOIN CAP_GFI_temp AS b ON (a.date28 = b.date28) AND (a.lig_cap = b.lig_cap) AND (a.no_bus_cap = b.no_bus_cap) "+
" AND a.voiture_cap = b.voiture_cap AND a.seconde28 >= SUBSTRING(b.hre_pre_dep,1,2)*3600 + SUBSTRING(b.hre_pre_dep,4,2)*60 + SUBSTRING(b.hre_pre_dep,7,2)-900 AND a.seconde28 <= SUBSTRING(b.hre_pre_dep,1,2)*3600 + SUBSTRING(b.hre_pre_dep,4,2)*60 + SUBSTRING(b.hre_pre_dep,7,2) "+
" WHERE a.source_imput is Null  AND  b.source_imput is Not Null;";

var MATCH19 = "UPDATE a "+
" SET a.periode=b.periode, a.no_bus = b.no_bus, a.voiture = b.voiture, a.ligne = b.ligne, a.source_imput = 'C31', a.hre_pre_dep = b.hre_pre_dep, a.hre_pre_arr = b.hre_pre_arr, a.direction = b.direction, a.trace = b.trace, a.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a LEFT JOIN CAP_GFI_temp AS b ON (a.date28 = b.date28) AND (a.lig_cap = b.lig_cap) AND (a.no_bus_cap = b.no_bus_cap) AND (a.voiture_cap = b.voiture_cap) "+
" AND (a.seconde28>=SUBSTRING(b.hre_pre_dep,1,2)*3600+SUBSTRING(b.hre_pre_dep,4,2)*60+SUBSTRING(b.hre_pre_dep,7,2)) AND (a.seconde28<=SUBSTRING(b.hre_pre_arr,1,2)*3600+SUBSTRING(b.hre_pre_arr,4,2)*60+SUBSTRING(b.hre_pre_arr,7,2)+900) "+
" WHERE a.source_imput is Null AND  b.source_imput is Not Null AND b.pos_course = 2;";
	
var MATCH20 = "UPDATE CAP_GFI_temp "+ 
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = no_bus_cap, " +
" CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'D10', CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction,"+ 
" CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.seconde28 >= b.sec_dep_course_pre AND a.seconde28 <=b.sec_arr_course_pre "+
" WHERE a.source_imput is Null AND b.no_bus is Null;";
	 
var MATCH21 = "UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = no_bus_cap, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'D11', "+
" CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.seconde28 >= b.sec_dep_course_pre-300 AND a.seconde28 <=b.sec_arr_course_pre "+
" WHERE a.source_imput is Null AND b.no_bus is Null;";
	
var MATCH22 = "UPDATE CAP_GFI_temp "+
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = no_bus_cap, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'E10', "+
" CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course "+
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.seconde28 >= b.sec_dep_course_pre AND a.seconde28 <=b.sec_arr_course_pre "+
" WHERE a.source_imput is Null;";
		
var MATCH23 = "UPDATE CAP_GFI_temp "+ 
" SET CAP_GFI_temp.periode=b.periode, CAP_GFI_temp.no_bus = no_bus_cap, CAP_GFI_temp.voiture = b.voiture, CAP_GFI_temp.ligne = b.ligne, CAP_GFI_temp.source_imput = 'E11', "+
" CAP_GFI_temp.hre_pre_dep = b.hre_pre_dep, CAP_GFI_temp.hre_pre_arr = b.hre_pre_arr, CAP_GFI_temp.direction = b.direction, CAP_GFI_temp.trace = b.trace, CAP_GFI_temp.pos_course = b.pos_course " +
" FROM CAP_GFI_temp AS a INNER JOIN bus_course AS b ON a.date28 = b.date_j AND a.lig_cap = b.ligne AND a.seconde28 >= b.sec_dep_course_pre-600 AND a.seconde28 <=b.sec_arr_course_pre "+
" WHERE a.source_imput is Null;";

var MATCH24 = "UPDATE CAP_GFI_temp "+
" SET periode='HP', no_bus = no_bus_cap, voiture = 'EXT', ligne = lig_cap, source_imput = 'EXT',"+
" hre_pre_dep = 'EXT_SCOL', hre_pre_arr = 'EXT_SCOL', direction = 'R' " +
" FROM CAP_GFI_temp  WHERE lig_cap Between 500 And 699 And DATEPART(dw, date28)=5 And seconde28 Between 43200 And 50400 And source_imput Is Null;";

var MATCH25 = "UPDATE a" +
	" SET periode=b.periode, a.no_bus = b.no_bus , a.voiture = b.voiture , a.ligne=b.ligne , a.source_imput = 'CHA' ,  a.hre_pre_dep=b.hre_pre_dep , "+
	" a.hre_pre_arr=b.hre_pre_arr , a.direction=b.direction , a.trace=b.trace , a.pos_course=b.pos_course "+
	" FROM CAP_GFI_temp AS a INNER JOIN CAP_GFI_temp as b ON a.date28 = b.date28 AND a.no_bus_cap = b.no_bus_cap AND a.chaine_val=b.chaine_val AND a.hre_pre_dep<b.hre_pre_dep " ;

//'-- 1ère étape création table temporaire chaine_validation ordonnanant¸les validations par course

var ALIMENTATION_CHAINE_VALIDATION = " INSERT INTO chaine_validation SELECT "+
	" '' as No, COUNT(b.seconde28) As ordre_valid, a.seconde28, a.identification, a.date28, convert(int,a.no_bus_cap) as no_bus_cap, '' as hre_pre_dep, '' as chaine_val "+
	" FROM extract_dbo_opus as a LEFT OUTER JOIN extract_dbo_opus as b ON a.date28=b.date28 AND convert(int, a.no_bus_cap)=convert(int, b.no_bus_cap) "+
	" AND a.seconde28 >= b.seconde28  "+
	" WHERE a.reseauoperation=33 and b.reseauoperation=33 "+
	" GROUP BY a.seconde28 ,a.identification ,a.date28 , convert(int, a.no_bus_cap) ;"+
	
	//ajout pour corriger la requete originale  
	//pour pouvoir faire la mise à jour de chaine_valid, on va créer une table temporaire 
	//avec la jointure sans le filtre temporel de 12 secondes de chaine_validation sur elle-même
 	" IF OBJECT_ID('chainetemp', 'U') IS NOT NULL DROP TABLE chainetemp; "+
	" select a.*, b.seconde28 as bseconde28, b.ordre_valid as bordre_valid "+
	" into chainetemp FROM chaine_validation as a LEFT OUTER JOIN chaine_validation as b ON "+
	" a.date28=b.date28 AND cast(a.no_bus_cap as int)= cast(b.no_bus_cap as int) AND a.ordre_valid = b.ordre_valid+1;"+
	
	" IF OBJECT_ID('chainetemp1', 'U') IS NOT NULL DROP TABLE chainetemp1; select *, ROW_NUMBER() OVER "+
	" (partition by ordre_valid, seconde28,identification,date28,no_bus_cap,hre_pre_dep,chaine_val "+
	" order by ordre_valid, seconde28,identification,date28,no_bus_cap,hre_pre_dep,chaine_val) as seq into chainetemp1 from chainetemp;"+
	" drop table chainetemp; select no,ordre_valid, seconde28,identification,date28,no_bus_cap,hre_pre_dep,chaine_val, bseconde28, "+
	" bordre_valid into chainetemp from chainetemp1 where seq=1;";
	
//'-- 2ème étape self join pour attribution d'un indice à chaque rupture de chaine de validation (12 secondes sans validation)	
	// Modifier car la requête originale a fait un Update sur une jointure à gauche
	// La requête est donc segmentée en deux: 
	// 1) voir table chainetemp définie dans Alimentation_chaine_validation. 
	// 2) On va y mettre à jour la colonne chaine_valid
	 
var ORDONNANCEMENT_VALIDATION_COURSE = "UPDATE chainetemp "+
	" SET chaine_val = -1 "+
 	" where seconde28-bseconde28>12 or bordre_valid is null;"+// paramètrable temporel ici
	" Drop table chaine_validation; Select top(1) no, ordre_valid, seconde28, identification, date28, no_bus_cap, hre_pre_dep, chaine_val "+
	" into chaine_validation From chainetemp; Truncate table chaine_validation; "+
	" insert into chaine_validation Select  no, ordre_valid, seconde28, identification, date28, no_bus_cap, hre_pre_dep, chaine_val "+
	" From chainetemp except select * from chaine_validation;" ;// skip dup

// 3ème étape self join pour ordonnancement des chaînes de validation
var CREATION_CHAINE_VALIDATION_2 = 
	" IF OBJECT_ID('chaine_validation2', 'U') IS NOT NULL DROP TABLE chaine_validation2; "+
	" SELECT a.ordre_valid ,a.identification ,a.date28 ,a.no_bus_cap ,a.seconde28 ,COUNT(b.seconde28) as chaine_val "+
	" INTO chaine_validation2 "+
	" FROM chaine_validation As a LEFT JOIN chaine_validation as b ON a.date28=b.date28 AND convert(int,a.no_bus_cap)=convert(int,b.no_bus_cap) "+
	" AND a.seconde28>=b.seconde28 "+
	" WHERE a.chaine_val = -1 AND b.chaine_val=-1 "+
	" GROUP BY a.ordre_valid ,a.identification ,a.date28 ,a.no_bus_cap ,a.seconde28; "; 

// 4ème étape self join pour attribuer le numéro de chaine à toutes les validations d une même chaîne
var CREATION_CHAINE_VALIDATION_3 = 
	" IF OBJECT_ID('chaine_validation3', 'U') IS NOT NULL DROP TABLE chaine_validation3; "+
	" SELECT a.ordre_valid ,a.identification ,a.date28 ,a.no_bus_cap ,a.seconde28, MAX(b.chaine_val) as chaine_val "+
	" INTO chaine_validation3 "+
	" FROM chaine_validation As a LEFT JOIN chaine_validation2 as b ON a.date28=b.date28 AND a.no_bus_cap=b.no_bus_cap AND a.seconde28>=b.seconde28 "+
	" WHERE a.chaine_val<>-1 "+
	" GROUP BY a.ordre_valid , a.identification , a.date28 , a.no_bus_cap , a.seconde28; ";

// 5ème étape UPDATE extract_dbo_opus avec chaine_validation2
var UPDATE_extract_dbo_opus_AVEC_CHAINE_VALID1 = 
	" UPDATE extract_dbo_opus "+
	" SET extract_dbo_opus.chaine_val = b.chaine_val  "+
	" FROM extract_dbo_opus as a "+ //left join
	" left JOIN chaine_validation2 as b ON a.identification=b.identification AND a.date28=b.date28 AND a.seconde28=b.seconde28 AND a.no_bus_cap=b.no_bus_cap "+
	" WHERE a.chaine_val=0 or a.chaine_val is null; "; 

// 6ème étape UPDATE extract_dbo_opus avec chaine_validation3
var UPDATE_extract_dbo_opus_AVEC_CHAINE_VALID2 = 
    " UPDATE extract_dbo_opus "+
	" SET extract_dbo_opus.chaine_val = b.chaine_val "+
	" FROM extract_dbo_opus as a "+ //left join
	" left JOIN chaine_validation3 as b ON a.identification=b.identification AND a.date28=b.date28 AND a.seconde28=b.seconde28 AND a.no_bus_cap=b.no_bus_cap "+
	" WHERE a.chaine_val=0 or a.chaine_val IS NULL; "; 

//'extraction données sdap_course_arret pour la période traitée 
var Extraction_sdap_course_arret =" INSERT INTO extract_sdap_course_arret  "+
	" SELECT assignation, CAST( concat(SUBSTRING(sdap_date,1,4),'-'&SUBSTRING(sdap_date,5,2),'-',SUBSTRING(sdap_date,7,2)) as date) AS date_j , "+
	" LTRIM(RTRIM(voiture)) AS voiture , hre_pre_dep , (SUBSTRING(hre_reel_arr,1,2)*3600+SUBSTRING(hre_reel_arr,4,2)*60+SUBSTRING(hre_reel_arr,7,2)) AS seconde_arr_arret , "+
	" SUBSTRING(hre_reel_dep,1,2)*3600+SUBSTRING(hre_reel_dep,4,2)*60+SUBSTRING(hre_reel_dep,7,2) AS seconde_dep_arret ,no_arret, position "+
	" FROM Stad.stad.sdap_course_arret "+
	" WHERE sdap_date between "+global.DATETxtDebut+" AND "+global.DATETxtFin+ " AND duree>0; ";
	
//'ajout des coordonnées des arrêts 
var Update_extract_sdap_course_arret = " Update extract_sdap_course_arret "+
	" SET extract_sdap_course_arret.coord_x = b.coord_x , extract_sdap_course_arret.coord_y=b.coord_y "+
	" FROM extract_sdap_course_arret As a INNER Join extract_ref_arret As b ON a.assignation=b.assignation AND a.no_arret=b.no_arret ;" ;
	
//'Extraction table ref_course_arret 
var Extraction_ref_course_arret;
           
//'Extraction table ref_course_prochain_arret 
var Extraction_ref_course_arret_suivant ;

//'Extraction table ref_course (pour détermination arrêts restants sur la ligne => arrêt débarquement) 
var Extraction_ref_course;
  
//'Extraction table ref_arret (pour détermination arrêts restants sur la ligne => arrêt débarquement) 
var Extraction_ref_arret = " INSERT INTO extract_ref_arret "+
	" SELECT distinct a.assignation , a.no_arret, a.coord_x , a.coord_y  "+
	" FROM Stad.stad.ref_arret AS a "+
	" INNER JOIN calendrier As b ON a.assignation=b.assignation "+
	" WHERE b.date_j BETWEEN "+ global.DATEDebut +" AND "+ global.DATEFin+" ; ";

//***************************** 'REQUETES MATCH ARRETS **********************************

/*'-- CODES IMPUTATION ARRET EMBARQUEMENT
	'----
	'---- 0 - NON IMPUTÉ (par défaut)
	'---- 1 - STAD (selon heure prévue à l'arrêt)
	'---- 2 - SDAP (selon heure réelle à l'arret)
	'---- 3 - HIST (selon habitude)
	'---- 4 - CHAI (selon enchaînement de montées)
	'---- 6 - L.45 (attribution automatique)
*/

//' ATTRIBUTION_AUTOMATIQUE_L.45
//' match avec direction de la course et arrêt de début de la course

var ATTRIBUTION_EMB_L45 = "Update a " +
	" SET a.no_arret_emb = b.no_arret , a.imput_emb=6 , a.position_arret_emb=b.position , a.coord_x_emb = b.coord_x , a.coord_y_emb = b.coord_y " +
	" FROM CAP_GFI_temp as a INNER Join extract_ref_course_arret As b ON Ltrim(rtrim(a.voiture))=Ltrim(rtrim(b.voiture)) AND a.hre_pre_dep=b.hre_pre_dep " +
	" WHERE a.ligne=45 AND b.info_acces='pre';";

//' ATTRIBUTION_ARRET_SDAP
//'match temporel entre validation et embarquement SDAP
var ATTRIBUTION_ARRET_SDAP = " UPDATE a " +
	" SET a.no_arret_emb=b.no_arret , a.imput_emb=2, a.position_arret_emb=b.position, a.coord_x_emb = b.coord_x, a.coord_y_emb = b.coord_y " +
	" From CAP_GFI_temp as a INNER JOIN extract_sdap_course_arret as b ON a.date24=b.date_j AND Ltrim(rtrim(a.voiture))=Ltrim(rtrim(b.voiture)) AND a.hre_pre_dep=b.hre_pre_dep "+
	" AND a.seconde28 >= b.seconde_arr_arret-10 AND a.seconde28 <= b.seconde_dep_arret+20 " + //parametrable temporel ici
	" WHERE a.no_arret_emb is null; "; 
	
//' ATTRIBUTION_ARRET_STAD_1
//'buffer de 40 secondes avant et après l'heure prévue à l'arrêt

//Determination_embarquement_possible ( liste des arrêts en fonction de la distance temporelle)
var Determination_embarquement_possible = " INSERT INTO arrets_possibles SELECT  "+
 " a.ID_D28_S28, null as [identification], b.no_arret as [no_arret], null as [no_arret_emb], null as [no_arret_deb], '' as [direction], b.position as [position], "+
 " null as [position_boucle], null as [position_arret_emb], b.coord_x as [coord_x], null as[coord_x_emb], b.coord_y as[coord_y], null as[coord_y_emb], "+
 " null as[nombre_occ], ABS(a.seconde28-b.seconde_hre_pre_arret) AS [proximite_temporelle], null as [proximite_arret] "+
 " FROM CAP_GFI_temp AS a INNER JOIN extract_ref_course_arret AS b ON a.assignation=b.assignation AND (a.voiture=b.voiture or a.voiture_CAP=b.voiture) "+
 " AND a.hre_pre_dep=b.hre_pre_dep "+
 " AND a.seconde28 >= b.seconde_hre_pre_arret-40 AND a.seconde28 <= b.seconde_hre_pre_arret+b.tmps_par_pre  "+ // parametrable temporel ici. Au lieu de considerer +40sec, on utilise plutot le temps de parcours prévu
 " WHERE a.no_arret_emb is null; ";
	
// Determination_embarquement_probable (retient l'arrêt le plus moins distant temporellement)
var Determination_embarquement_probable = " INSERT INTO arrets_probables SELECT "+
	" ID_D28_S28 , null as [proximite_arret], MIN(proximite_temporelle) as proximite_temporelle, null as [nombre_occ], '' as direction " +
	" FROM arrets_possibles GROUP BY ID_D28_S28 ;";

//'UPDATE_CAP_GFI_embarq_stad
var UPDATE_CAP_GFI_embarq_stad = " UPDATE CAP_GFI_temp " +
	" SET CAP_GFI_temp.no_arret_emb=c.no_arret , CAP_GFI_temp.imput_emb=1 , CAP_GFI_temp.position_arret_emb=c.position , "+
	" CAP_GFI_temp.coord_x_emb = c.coord_x , CAP_GFI_temp.coord_y_emb = c.coord_y "+
	" From CAP_GFI_temp as a INNER JOIN arrets_probables AS b ON a.ID_D28_S28=b.ID_D28_S28 " +
	" INNER JOIN arrets_possibles as c ON b.ID_D28_S28=c.ID_D28_S28 AND b.proximite_temporelle=c.proximite_temporelle "+
	" where a.no_arret_emb is null; " ;

//' ATTRIBUTION_ARRET_HISTORIQUE
//  'hypothèse : si à une date différente (dans les 21 jours précédents), un même utilisateur embarque sur une même ligne à une heure proche d'un embarquement déjà localisé 
//	alors c'est très probablement le même arrêt >>> jointure avec ref_course_arret pour s'assurer que l'arrêt existe toujours

var Determination_historique;

var Determination_meilleur_historique = " INSERT INTO arrets_probables SELECT ID_D28_S28, null as [proximite_arret], null as [proximite_temporelle], "+
	" MAX(nombre_occ) AS nombre_occ ,  '' as direction FROM arrets_possibles GROUP BY ID_D28_S28 ;";

var UPDATE_CAP_GFI_embarq_histo = "UPDATE CAP_GFI_temp " +
	" SET CAP_GFI_temp.no_arret_emb=c.no_arret_emb ,CAP_GFI_temp.position_arret_emb=c.position_arret_emb ,CAP_GFI_temp.coord_x_emb=c.coord_x_emb ,"+
	" CAP_GFI_temp.coord_y_emb=c.coord_y_emb ,CAP_GFI_temp.imput_emb=3 "+
	" FROM  CAP_GFI_temp as a INNER JOIN arrets_probables as b ON a.ID_D28_S28=b.ID_D28_S28 " +
	" INNER JOIN arrets_possibles as c ON b.ID_D28_S28=c.ID_D28_S28 AND b.nombre_occ=c.nombre_occ ;" ;
	
//' ATTRIBUTION_ARRET_CHAINAGE (selon les enchaînements de montée de voyageurs)
//' hypothèse : toutes les validations d'une même chaine se font à un même arrêt
        
var ATTRIBUTION_ARRET_CHAINAGE = " UPDATE a " +
	" SET a.no_arret_emb=b.no_arret_emb , a.imput_emb=4 , a.position_arret_emb=b.position_arret_emb , a.coord_x_emb = b.coord_x_emb , "+
	" a.coord_y_emb = b.coord_y_emb FROM CAP_GFI_temp as a INNER JOIN CAP_GFI_temp as b ON a.no_bus_CAP=b.no_bus_CAP AND a.chaine_val=b.chaine_val "+
	" AND a.ligne=b.ligne WHERE a.imput_emb<2 AND b.imput_emb=2 ";

var ATTRIBUTION_ARRET_CHAINAGE_STAD = " UPDATE a " +
	" SET a.no_arret_emb=b.no_arret_emb , a.imput_emb=1 , a.position_arret_emb=b.position_arret_emb , a.coord_x_emb = b.coord_x_emb , "+
	" a.coord_y_emb = b.coord_y_emb FROM CAP_GFI_temp AS a INNER JOIN CAP_GFI_temp as b ON a.no_bus_CAP=b.no_bus_CAP AND a.chaine_val=b.chaine_val AND "+
	" a.ligne=b.ligne WHERE a.imput_emb is null AND b.imput_emb=1 ;";
    
//'2-match avec buffer de 15 minutes avant l'heure de début de la course
var ATTRIBUTION_ARRET_STAD_2 = " UPDATE CAP_GFI_temp " +
	" SET CAP_GFI_temp.no_arret_emb=b.no_arret ,CAP_GFI_temp.imput_emb=1 , CAP_GFI_temp.position_arret_emb=b.position, "+
	" CAP_GFI_temp.coord_x_emb = b.coord_x, CAP_GFI_temp.coord_y_emb = b.coord_y " +
	" FROM CAP_GFI_temp as a INNER JOIN extract_ref_course_arret as b ON a.assignation=b.assignation AND a.type_service=b.type_service AND (a.voiture_CAP=b.voiture OR a.voiture=b.voiture) "+ 
	" AND a.hre_pre_dep=b.hre_pre_dep AND a.seconde28 >= b.seconde_hre_pre_dep-900 AND a.seconde28 < b.seconde_hre_pre_dep " + //parametrable temporel ici
	" WHERE a.no_arret_emb is null AND b.info_acces = 'pre' ;";	

//match basé sur horaire planifié, no ligne, no voiture et assignation et jointure additionnelle pour coordonnées des arrets	
var ATTRIBUTION_ARRET_STAD_3 = " UPDATE CAP_GFI_temp SET CAP_GFI_temp.no_arret_emb=b.no_arret ,CAP_GFI_temp.imput_emb=1 , CAP_GFI_temp.position_arret_emb=b.position "+
	" FROM CAP_GFI_temp as a INNER JOIN (select * FROM [Stad].[stad].[ref_course_arret]) as b ON a.assignation=b.assignation AND a.type_service=b.type_service "+
	" AND (a.voiture_CAP=b.voiture OR a.voiture=b.voiture) AND a.lig_CAP=b.ligne AND a.seconde28 between "+
	" SUBSTRING(b.hre_pre_arret,1,2)*3600+SUBSTRING(b.hre_pre_arret,4,2)*60+SUBSTRING(b.hre_pre_arret,7,2)-20 AND "+
	" SUBSTRING(b.hre_pre_arret,1,2)*3600+SUBSTRING(b.hre_pre_arret,4,2)*60+SUBSTRING(b.hre_pre_arret,7,2)+b.tmps_par_pre WHERE a.no_arret_emb is null; "+ //adjustabe -20 et +b.tmps_par_pre  (le temps de parcours prévu au lieu des 20s)
	" update CAP_GFI_temp set CAP_GFI_temp.coord_x_emb= b.coord_x, CAP_GFI_temp.coord_y_emb = b.coord_y "+
	" FROM CAP_GFI_temp as a INNER JOIN stad.stad.ref_arret as b On a.no_arret_emb=b.no_arret and a.assignation=b.assignation where a.coord_x_emb is null ; ";

//match basé sur horaire planifié, no ligne, no voiture et assignation et jointure additionnelle pour coordonnées des arrets	
var ATTRIBUTION_ARRET_STAD_4 = " UPDATE CAP_GFI_temp SET CAP_GFI_temp.no_arret_emb=b.no_arret ,CAP_GFI_temp.imput_emb=1 , CAP_GFI_temp.position_arret_emb=b.position "+
	" FROM CAP_GFI_temp as a INNER JOIN (select * FROM [Stad].[stad].[ref_course_arret]) as b ON a.assignation=b.assignation AND a.type_service=b.type_service AND (a.voiture_CAP=b.voiture OR a.voiture=b.voiture) "+ 
	" AND a.lig_CAP=b.ligne AND a.seconde28 between SUBSTRING(b.hre_pre_arret,1,2)*3600+SUBSTRING(b.hre_pre_arret,4,2)*60+SUBSTRING(b.hre_pre_arret,7,2)-20 AND "+
	" SUBSTRING(b.hre_pre_arret,1,2)*3600+SUBSTRING(b.hre_pre_arret,4,2)*60+SUBSTRING(b.hre_pre_arret,7,2)+b.tmps_par_pre WHERE a.no_arret_emb is null; "+ //adjustabe -20 et + b.tmps_par_pre
	" update CAP_GFI_temp set CAP_GFI_temp.coord_x_emb= b.coord_x, CAP_GFI_temp.coord_y_emb = b.coord_y "+
	" FROM CAP_GFI_temp as a INNER JOIN stad.stad.ref_arret as b On a.no_arret_emb=b.no_arret and a.assignation=b.assignation where a.coord_x_emb is null ; ";	

 
//'attribution d'arrêt d'embarquement uniquement pour les données de la période en cours de traitement
     
//'---correction embarquement au dernier arrêt d'une course
//'Correction validation dernier arrêt
var CORRECTION_VALID_DER_ARRET = " UPDATE a " +
	" SET a.ligne=b.ligne_suivant ,a.hre_pre_dep=b.hre_pre_dep_suivant ,a.hre_pre_arr=b.hre_pre_arr_suivant ,a.trace=b.trace_suivant ,a.direction=b.direction_suivant , "+
	" a.no_arret_emb=b.no_arret_suivant, a.position_arret_emb=b.position_suivant, a.periode=b.periode, a.coord_x_emb = b.coord_x_suivant , a.coord_y_emb = b.coord_y_suivant "+
	" FROM  CAP_GFI_temp as a INNER JOIN extract_ref_course_arret_suivant as b ON (a.lig_CAP=b.ligne OR a.ligne=b.ligne)  AND (a.voiture_CAP=b.voiture OR a.voiture=b.voiture) AND a.hre_pre_dep=b.hre_pre_dep "+
	" AND a.no_arret_emb=b.no_arret AND a.assignation=b.assignation where a.no_arret_emb is null; "; 

 
/*' -- CODES IMPUTATION ARRET DEBARQUEMENT
'----
'---- 0 - NON IMPUTÉ (par défaut)
'---- 1 - VALIDATION SUIVANTE MÉTRO
'---- 2 - VALIDATION SUIVANTE BUS
'---- 3 - RETOUR AU DOMICILE
'---- 4 - DÉPART DU JOUR SUIVANT
'---- 5 - HISTORIQUE DE L'UTILISATEUR
'---- 6 - L.45 (attribution automatique)
 */   
    
//' ATTRIBUTION_AUTOMATIQUE_L.45
//' match avec direction de la course et arrêt de fin de la course

var ATTRIBUTION_DEB_L45 = " Update CAP_GFI_temp " +
" SET CAP_GFI_temp.no_arret_deb = b.no_arret , CAP_GFI_temp.imput_deb=6 " +
" FROM CAP_GFI_temp as a INNER Join extract_ref_course_arret As b ON  Rtrim(Ltrim(a.voiture))= Rtrim(Ltrim(b.voiture)) "+
" AND a.hre_pre_dep=b.hre_pre_dep " +
" WHERE a.ligne=45 AND b.info_acces='der';";

//' Série de requête servant à déterminer la "ligne de fuite" : liste des arrêts situés après l'arrêt d'embarquement
//' la subtilité est de prendre en compte les cas où le terminus de bout de ligne est traversant (boucle) et que l'usager peut donc être descendu à un arrêt correspond à la course suivante...
    
//' alimentation table listant les courses et indiquant le départ suivant lorsque celui-ci referme une boucle
var ALIMENTATION_BOUCLE_COURSE = " INSERT INTO boucle_course " +
" SELECT a.date_j , a.assignation , a.type_service , a.voiture , a.ligne , a.hre_pre_dep , b.hre_pre_dep AS dep_suivant_boucle , a.direction , a.trace , a.pos_arr " +
" FROM extract_ref_course as a LEFT JOIN extract_ref_course as b ON a.date_j=b.date_j AND a.ligne=b.ligne "+
" AND  Rtrim(Ltrim(a.voiture))= Rtrim(Ltrim(b.voiture)) "+
" AND a.no_seq_course=b.no_seq_course-1 AND a.no_voyage=b.no_voyage; ";
  
//' alimentation table extract_liste_arret_boucle associant l'ensemble des arrêts à chacune des courses, y compris les arrêts de la course suivante lorsque celui-ci referme une boucle
var ALIMENTATION_LISTE_ARRET_BOUCLE = " INSERT INTO liste_arret_boucle SELECT distinct a.date_j, a.assignation, a.type_service, a.voiture, a.ligne, b.hre_pre_dep, b.dep_suivant_boucle, "+
" a.direction, b.trace, IIF(a.hre_pre_dep=b.hre_pre_dep,a.position,a.position+b.pos_arr) AS position_boucle, a.coord_x, a.coord_y, "+
" a.seconde_hre_pre_arret, null as seconde_hre_reel_arret, a.no_arret, null as reseauoperation "+
" FROM extract_ref_course_arret as a INNER JOIN boucle_course as b ON a.date_j=b.date_j "+
" AND a.ligne=b.ligne AND  Rtrim(Ltrim(a.voiture))= Rtrim(Ltrim(b.voiture)) AND (a.hre_pre_dep=b.hre_pre_dep OR a.hre_pre_dep=b.dep_suivant_boucle) ;";
   
//' ajout des heures réelles de passage à l'arrêt à la table extract_liste_arret_boucle
var UPDATE_LISTE_ARRET_BOUCLE = " UPDATE liste_arret_boucle " +
" SET liste_arret_boucle.seconde_hre_reel_arret = b.seconde_dep_arret "+ 
" FROM liste_arret_boucle AS a left JOIN extract_sdap_course_arret as b ON a.date_j=b.date_j "+
" AND Rtrim(Ltrim(a.voiture))= Rtrim(Ltrim(b.voiture)) "+
" AND a.no_arret=b.no_arret AND (a.hre_pre_dep=b.hre_pre_dep OR a.dep_suivant_boucle=b.hre_pre_dep) ;" ;
	
//'attribution de la valeur -1 aux arrêts non desservis des courses faites par des bus équipés SDAP
var UPDATE_INDICE_ARRETS_PORTES_FERMEES = " UPDATE liste_arret_boucle " +
" SET liste_arret_boucle.seconde_hre_reel_arret = -1 " +
" FROM liste_arret_boucle as a INNER JOIN extract_sdap_course as b ON a.date_j=b.date_j AND "+
" Rtrim(Ltrim(a.voiture))= Rtrim(Ltrim(b.voiture)) "+
" AND (a.hre_pre_dep=b.hre_pre_dep OR a.dep_suivant_boucle=b.hre_pre_dep) " +
" WHERE a.seconde_hre_reel_arret IS NULL";

//'alimentation de la table listant les validations sur le RTL suivies d'une validation à une station de métro
var ALIMENTATION_validations_successives_metro = " INSERT INTO validations_successives "+
" SELECT a.ID_D28_S28 ,a.identification ,a.date28 ,a.voiture ,a.hre_pre_dep AS hre_pre_dep_1 ,a.seconde28 AS seconde_1 "+
" ,a.no_arret_emb AS no_arret_emb_1 ,a.position_arret_emb AS position_1 ,b.seconde28 AS seconde_2 ,b.station AS no_arret_emb_2 "+
" ,b.coord_x AS coord_x_emb_2 ,b.coord_y AS coord_y_emb_2 FROM CAP_GFI_temp as a "+
" INNER JOIN BD_VALID_METRO as b ON a.date28=b.date28 AND a.identification=b.identification AND a.ordre_valid_total=b.ordre_valid_total-1 "+
" WHERE a.no_arret_emb is not null AND a.no_arret_deb is null;"; 

//'détermination de l'arrêt de débarquement possible au métro suivant l'heure réelle et prévue et la proximité (buffer 500m)
var Debarquements_possibles_metro_hre_reel_pre = "IF OBJECT_ID('dbo.temp01', 'U') IS NOT NULL drop table temp01; with temp01 as ( "+
" SELECT a.ID_D28_S28  ,b.no_arret AS no_arret_deb , power(a.coord_x_emb_2-b.coord_x,2)+power(a.coord_y_emb_2-b.coord_y,2) AS proximite_arret , "+
" b.direction ,b.position_boucle ,b.coord_x ,b.coord_y   "+
" FROM  validations_successives as a INNER JOIN liste_arret_boucle as b ON a.date28=b.date_j AND  Rtrim(Ltrim(a.voiture))= Rtrim(Ltrim(b.voiture))AND "+
" a.hre_pre_dep_1=b.hre_pre_dep AND a.position_1<b.position_boucle AND a.seconde_2> b.seconde_hre_reel_arret-600 "+
" WHERE b.seconde_hre_reel_arret IS NOT NULL AND b.seconde_hre_reel_arret<>-1 AND power(a.coord_x_emb_2-b.coord_x,2)+power(a.coord_y_emb_2-b.coord_y,2)<250000   "+
" UNION  SELECT ID_D28_S28 ,b.no_arret AS no_arret_deb ,power(a.coord_x_emb_2-b.coord_x,2)+power(a.coord_y_emb_2-b.coord_y,2) AS proximite_arret ,b.direction ,"+
" b.position_boucle ,b.coord_x ,b.coord_y "+
" FROM validations_successives as a INNER JOIN liste_arret_boucle as b ON a.date28=b.date_j AND  Rtrim(Ltrim(a.voiture))= Rtrim(Ltrim(b.voiture)) "+
"  AND a.hre_pre_dep_1=b.hre_pre_dep AND a.position_1<b.position_boucle  AND a.seconde_2> b.seconde_hre_pre_arret-600  "+
" WHERE b.seconde_hre_reel_arret IS NULL AND power(a.coord_x_emb_2-b.coord_x,2)+power(a.coord_y_emb_2-b.coord_y,2)<250000 "+
" ) INSERT INTO arrets_possibles select [ID_D28_S28], null as [identification] ,null as [no_arret], null as [no_arret_emb] ,"+
" [no_arret_deb] ,[direction] "+
" ,null as [position],[position_boucle] , null as [position_arret_emb] ,[coord_x], null as [coord_x_emb],[coord_y] ,"+
" null as[coord_y_emb], null as [nombre_occ],null as [proximite_temporelle],[proximite_arret] from temp01; IF OBJECT_ID('dbo.temp01', 'U') "+
" IS NOT NULL drop table temp01;";

//'alimentation de la table listant les validations sur le RTL suivies d'une autre validation sur le RTL
var ALIMENTATION_validations_successives_bus = " Truncate table validations_successives; "+ 
" INSERT INTO  validations_successives   " +
" SELECT  a.ID_D28_S28  ,a.identification  ,a.date28  ,a.voiture  ,a.hre_pre_dep   AS hre_pre_dep_1  ,a.seconde28   AS seconde_1   " +
" ,a.no_arret_emb AS no_arret_emb_1  ,a.position_arret_emb  AS position_1  ,b.seconde28   AS seconde_2  ,b.no_arret_emb   AS no_arret_emb_2  " +
" ,b.coord_x_emb AS coord_x_emb_2,b.coord_y_emb AS coord_y_emb_2   " +
" FROM CAP_GFI_temp as a   " +
" INNER JOIN CAP_GFI_temp as b ON a.date28=b.date28 AND a.identification=b.identification AND a.ordre_valid_total=b.ordre_valid_total-1   " +
" WHERE a.no_arret_emb IS NOT NULL AND b.no_arret_emb IS NOT NULL AND a.no_arret_deb IS NULL ";

//'détermination de l'arrêt de débarquement possible dans l'agglo suivant l'heure réelle et prévue et la proximité (buffer 500m)
var Debarquements_possibles_RTL_hre_reel_pre = "IF OBJECT_ID('dbo.temp01', 'U') IS NOT NULL drop table temp01; with temp01 as ( "+
" SELECT ID_D28_S28, b.no_arret AS no_arret_deb, power(a.coord_x_emb_2-b.coord_x,2)+ power(a.coord_y_emb_2-b.coord_y, 2) AS proximite_arret "+
" ,b.direction ,b.position_boucle ,b.coord_x ,b.coord_y FROM validations_successives as a INNER JOIN liste_arret_boucle "+
" as b ON a.date28=b.date_j AND  Rtrim(Ltrim(a.voiture))= Rtrim(Ltrim(b.voiture)) "+
"  AND a.hre_pre_dep_1=b.hre_pre_dep AND a.position_1<b.position_boucle  AND a.seconde_2> b.seconde_hre_reel_arret "+ 
" WHERE  b.seconde_hre_reel_arret IS NOT NULL AND b.seconde_hre_reel_arret<>-1 AND power(a.coord_x_emb_2-b.coord_x,2)+ power(a.coord_y_emb_2-b.coord_y, 2)<250000 "+
" UNION "+
" SELECT ID_D28_S28,b.no_arret AS no_arret_deb,power(a.coord_x_emb_2-b.coord_x,2)+power(a.coord_y_emb_2-b.coord_y,2) AS proximite_arret ,b.direction ,b.position_boucle ,b.coord_x ,b.coord_y   "+
" FROM validations_successives as a "+
" INNER JOIN liste_arret_boucle as b ON a.date28=b.date_j AND  Rtrim(Ltrim(a.voiture))= Rtrim(Ltrim(b.voiture)) "+
"  AND a.hre_pre_dep_1=b.hre_pre_dep AND a.position_1<b.position_boucle AND a.seconde_2> b.seconde_hre_pre_arret-500   "+ // filtre temporel ajustable 
" WHERE b.seconde_hre_reel_arret IS NULL  AND power(a.coord_x_emb_2-b.coord_x, 2)+ power(a.coord_y_emb_2-b.coord_y,2)<250000 "+
" ) INSERT INTO arrets_possibles  select [ID_D28_S28], null as [identification] ,null as [no_arret], null as [no_arret_emb],[no_arret_deb],[direction] "+
" ,null as [position],[position_boucle] , null as [position_arret_emb],[coord_x] , null as [coord_x_emb] ,[coord_y], null as[coord_y_emb], null as [nombre_occ] "+
" ,null as [proximite_temporelle] ,[proximite_arret] from temp01; IF OBJECT_ID('dbo.temp01', 'U') IS NOT NULL drop table temp01;";
		
//'détermination de l'arrêt de débarquement probable (le plus proche parmi les possibilités)
var Determination_debarquement_probable = "Truncate table arrets_probables;"+  
" INSERT INTO arrets_probables " +
" SELECT ID_D28_S28, MIN(proximite_arret) as proximite_arret, null as proximite_temporelle, null as nombre_occ, direction " +
" FROM arrets_possibles GROUP BY ID_D28_S28, direction "; 

//'écriture des arrêts de débarquement sur la table CAP_GFI
var UPDATE_CAP_GFI_debarq;                       

//'écriture des arrêts de débarquement sur la table CAP_GFI (dernier débarquement de la veille) ??????????????? where is the call
var UPDATE_serveur_debarq = "UPDATE dbo.CAP_GFI_" + global.ANNEEopus4 +
" SET a.no_arret_deb=c.no_arret_deb , a.imput_deb= " + global.Code_imputation_debarquement + 
" FROM dbo.CAP_GFI_" + global.ANNEEopus4 + " as a INNER JOIN arrets_probables as b ON a.ID_D28_s28= b.ID_D28_s28  " +
" INNER JOIN arrets_possibles as c ON b.ID_D28_S28=c.ID_D28_S28 AND b.proximite_arret=c.proximite_arret  " +
" WHERE a.imput_deb is null; ";

//'table des dernières validations (détermination du débarquement selon la méthode du retour au domicile)
var ALIMENTATION_table_dernieres_validations =  "Truncate table dernieres_validations;"+  
" INSERT INTO dernieres_validations "+
" SELECT a.[ID_D28_S28]	,a.[date28]	,a.[identification]	,a.[voiture] ,a.[hre_pre_dep] "+
" ,a.[no_arret_emb],a.[position_arret_emb] , null as [ligne] ,null as [direction] ,null as [trace] "+
" FROM  CAP_GFI_temp as a "+
" INNER JOIN (SELECT identification,date28,MAX(ordre_valid_total) AS ordre_valid_total FROM CAP_GFI_temp GROUP BY identification,date28 HAVING MAX(ordre_valid_total)>1) as b "+
" ON a.identification=b.identification AND a.date28=b.date28 AND a.ordre_valid_total=b.ordre_valid_total "+
" WHERE a.imput_deb is null; ";

//'alimentation de la table combinant les dernières validations de la journée avec les 1ères validations de la journée
var ALIMENTATION_validations_successives_retour_dom = " Truncate table validations_successives; "+ 
" INSERT INTO validations_successives "+
" SELECT  a.ID_D28_S28 ,a.identification ,a.date28 ,a.voiture ,a.hre_pre_dep AS hre_pre_dep_1 , null as [seconde_1], a.no_arret_emb AS no_arret_emb_1 , "+
" a.position_arret_emb AS position_1  , null as [seconde_2],  b.no_arret_emb   AS no_arret_emb_2  ,b.coord_x_emb   AS coord_x_emb_2  ,b.coord_y_emb   AS coord_y_emb_2 "+
" FROM   dernieres_validations as a INNER JOIN CAP_GFI_temp as b   ON a.identification=b.identification "+
" WHERE   b.ordre_valid_total=1  AND a.no_arret_emb is not null  AND b.no_arret_emb is not null;";

//'détermination de l'arrêt de débarquement possible suivant la proximité (buffer 500m)
var Debarquements_possibles_Retour_Domicile = " INSERT INTO  arrets_possibles  SELECT a.[ID_D28_S28] , null as [identification]  ,null as [no_arret] ,null as [no_arret_emb] ,b.no_arret AS no_arret_deb  ,b.[direction] "+
 " , null as [position] ,b.[position_boucle]  ,null as [position_arret_emb] ,b.[coord_x], null as [coord_x_emb], b.[coord_y] , null as [coord_y_emb] "+
 " , null as [nombre_occ], null as [proximite_temporelle], power(a.coord_x_emb_2-b.coord_x,2)+ power(a.coord_y_emb_2-b.coord_y,2) AS proximite_arret "+
 " FROM validations_successives as a  INNER JOIN liste_arret_boucle as b  ON  Rtrim(Ltrim(a.voiture))= Rtrim(Ltrim(b.voiture)) "+
"   AND a.hre_pre_dep_1=b.hre_pre_dep AND a.position_1<b.position_boucle "+
 " WHERE power(a.coord_x_emb_2-b.coord_x,2)+ power(a.coord_y_emb_2-b.coord_y, 2)<250000; ";

//table des dernières validations (détermination du débarquement selon la méthode du 1er déplacement du jour suivant)  ??????????? EXECUTION NON REPEREE ?????????????
var ALIMENTATION_table_dernieres_validations_veille = " INSERT INTO dernieres_validations  " +
	" SELECT a.ID_D28_S28 , convert(date, cast(a.date28 as datetime)) as date28,a.identification ,a.voiture, a.hre_pre_dep ,a.no_arret_emb , a.position_arret_emb, a.ligne , a.direction ,a.trace "+
	" FROM dbo.CAP_GFI_" + global.ANNEEopus4 + " as a INNER JOIN (SELECT identification,convert(date, cast(date28 as datetime)), MAX(ordre_valid_total) AS ordre_valid_total "+
	" FROM dbo.CAP_GFI_" + global.ANNEEopus4 + " WHERE convert(date, cast(date28 as datetime))=" + global.DATEDebut + "-1 GROUP BY identification, convert(date, cast(date28 as datetime)) HAVING MAX(ordre_valid_total)>1) as b "+
	" ON a.identification=b.identification AND a.ordre_valid_total=b.ordre_valid_total "+
	" WHERE convert(date, cast(a.date28 as datetime))=" + global.DATEDebut + "-1 AND a.imput_deb is null AND a.imput_emb > 0 ";

//' ATTRIBUTION ARRET DÉBARQUEMENT SELON L'HISTORIQUE
//'hypothèse : si à une date différente (dans les 21 jours précédents), un même utilisateur embarque au même arrêt sur une même ligne à une heure proche alors il est probablement descendu au même arrêt
//'jointure avec extract_ref_course_arret pour s'assurer que l'arrêt existe

var Determination_historique_deb = "INSERT INTO arrets_possibles " +
"SELECT a.ID_D28_S28 ,b.identification , null as no_arret, null as no_arret_emb, b.no_arret_deb , null as direction, null as position, COUNT(*) AS nombre_occ " +
"FROM (CAP_GFI_temp AS a " +
"INNER JOIN dbo.CAP_GFI_" +global.ANNEEopus4 + " AS b ON a.identification=b.identification AND a.date28-21 <= b.date28 AND a.date28 > b.date28 AND a.no_arret_emb=b.no_arret_emb AND a.seconde28-600 <= b.seconde28 AND a.seconde28+600 >= b.seconde28 AND (a.ligne=b.ligne or a.ligne=b.lig_cap or a.lig_cap=b.ligne) AND a.direction=b.direction) " +
"INNER JOIN extract_ref_course_arret as c ON ( Rtrim(Ltrim(a.voiture))= Rtrim(Ltrim(b.voiture)) "+
" OR  Rtrim(Ltrim(a.voiture_cap))= Rtrim(Ltrim(b.voiture)) ) AND a.hre_pre_dep=c.hre_pre_dep AND b.no_arret_emb=c.no_arret " +
"WHERE b.imput_deb is not null AND a.imput_deb is null GROUP BY a.ID_D28_S28 ,b.identification ,b.no_arret_deb";

var UPDATE_CAP_GFI_debarq_histo = " UPDATE CAP_GFI_temp SET CAP_GFI_temp.no_arret_deb=c.no_arret_deb ,CAP_GFI_temp.imput_deb=5 " + 
" FROM CAP_GFI_temp as a INNER JOIN arrets_probables as b ON a.ID_D28_S28=b.ID_D28_S28 " +
" INNER JOIN arrets_possibles as c ON b.ID_D28_S28=c.ID_D28_S28 AND b.nombre_occ=c.nombre_occ " +
" WHERE a.imput_deb is null; ";
                                
//' Correction de l'arrêt de débarquement (le remplacer par 0) s'il est identique à l'arrêt d'embarquement
var Correction_emb_egal_deb = " UPDATE CAP_GFI_temp " +
" SET no_arret_deb=0, imput_deb=0 WHERE no_arret_emb=no_arret_deb;"; //ajouter par KT -- and no_arret_deb is not null
                            
//' CORRECTION DE L'ARRÊT DE DÉBARQUEMENT ATTRIBUÉ (LE REMPLACER PAR 0) S'IL EST SITUÉ À MOINS DE 300m DE L'ARRÊT D'EMBARQUEMENT
var Correction_emb_proche_deb = " Update CAP_GFI_temp SET no_arret_deb = 0, imput_deb=0 FROM CAP_GFI_temp as a "+
" INNER Join extract_ref_arret As b ON a.no_arret_deb=b.no_arret " +
" WHERE power(a.coord_x_emb-b.coord_x, 2)+power(a.coord_y_emb-b.coord_y,2)<90000; ";                           

//'RQ transfert des données traitées sur la base de données finale située sur le serveur
var Transfert_BD_OPUS = "INSERT INTO [AchalDep].[dbo].CAP_GFI_"+global.ANNEEopus4+ " SELECT * FROM  [AchalDep].[dbo].[CAP_GFI_temp]";
						 
//**************************************** FIN DES REQUETES SQL  ****************************//

/////////////////////////////////////////////////////////
//               		LISTE DE FONCTIONS     		  //   
///////////////////////////////////////////////////////

function Temps_Ecoule(){
	global.Moment = (new Date()) - LeDebut;  //LeDebut n'a pas encore de valeur affectée (ce sera le timestamp au début du lancement du processus)
	return global.Moment;	
};

//Fonction pour extraire le jour de la semaine d'une date dd
function Jour_de_la_semaine(){
	let dod = new Date(global.dateDebut).getDay()+1;    // DATEDebut n'a pas encore de valeur affectée
	switch (dod){
		case 1:	global.JourSemaine = "Dimanche";
		case 2: global.JourSemaine = "Lundi";
		case 3: global.JourSemaine = "Mardi";
		case 4:	global.JourSemaine = "Mercredi";
		case 5:	global.JourSemaine = "Jeudi";
		case 6:	global.JourSemaine = "Vendredi";
		case 7:	global.JourSemaine = "Samedi";
		default:'' ;
	}
	console.log('\n');
	return  new Promise( function(resolve, reject){
		setTimeout ( function(){
			console.log("\n"+ftimestamp(new Date())+" #5- promise5 - Jour_de_la_semaine complété\n ");
			resolve(VerifJour());
		},5000)
	})
};

//Function pour compter le nombre de fois la procédure est roulée	
function VerifJour(){	
	console.log(ftimestamp(ftimestamp(new Date()))+ ' #5a - Promise 5 Verification de la journée');
	global.NbDeTraitement++; //'Nombre de fois que la procédure est roulée automatiquement ----- depuis l'ouverture d'Access
	let d0d=new Date().getDay()+1;
	
	if(d0d<8){ //toujours vrai
		console.log('On est jour no '+ d0d +" de la semaine. Le lancement du processus d'intégration des données est accepté!");
		global.Fonctionnement_auto = 1; //'Indicateur précisant que le traitement se fait dans le cadre du déclenchement automatique les soirs et fins de semaine
		try {
			COMPILATION_OPUS_GFI(); // Appel une liste de fonctions et exec sql queries
			console.log("\n"+ftimestamp(new Date())+" #6 - promise 6 - Le processus d'intégration est lancé\n");
		}
		catch(err) { 
			console.log(err); 
		};
	}
	else { 
		Fonctionnement_auto = 0;
		console.log('Le jobber Compilation_Opus_GFI ne peut pas être appelé automatiquement pour le moment. Veuillez essayer une prochaine fois');
	}
};	

//'Nettoyage de toutes les tables temporaires 	
function Nettoyage_toutes_tables(){
	//Liste des requêtes SQL pour vider les tables temporaires
	let table_temp = [
		Vidange_table_temporaire_1,
		Vidange_table_temporaire_2,
		Vidange_table_temporaire_3,
		Vidange_table_temporaire_4,
		Vidange_table_temporaire_6,
		Vidange_table_temporaire_7,
		Vidange_table_temporaire_8,
		Vidange_table_temporaire_9,
		Vidange_table_temporaire_10,
		Vidange_table_temporaire_11,
		Vidange_table_temporaire_12,
		Vidange_table_temporaire_13,
		Vidange_table_temporaire_14,
		Vidange_table_temporaire_15,
		Vidange_table_temporaire_16,
		Vidange_table_temporaire_17,
		Vidange_table_temporaire_18,
		Vidange_table_temporaire_19,
		Vidange_table_temporaire_20,
		Vidange_table_temporaire_21,
		Vidange_table_temporaire_22,
		Vidange_table_temporaire_23,
		Vidange_table_temporaire_24
	];
	sql.close(); 	
	sql.connect(config, function (err){
		try {
			let request = new sql.Request();
			table_temp.forEach(function(table){
				request.query(table, function (err, getdata) {
					if (err) console.log(err); 
					else{ 
						console.log(new Date()+': Etape 01 - Table', table, 'effacée avec succès');
						fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
							'\n'+new Date()+' : Etape 01 - Table '+ table+' effacée avec succès'
							,function (err) {
								if (err) throw err;
							}
						);
					}
				});
			})		
		} 
		catch(err) { 
			console.log(err);
		};
	})
};	


//'----------------------------------------------
//'ATTRIBUATION D'UNE COURSE AUX VALIDATIONS OPUS

//'Mise à jour du calendrier et Extraction des données sur la plage de temps traitée
async function Requetes_SQL(){	
	console.log("\n"+ftimestamp(new Date())+' Promise 6.33 -- la fonction Requêtes_SQL est appelée - remplissage des tables temporaires');
	let Extraction_sdap_co;
	
	Extraction_dbo_Opus = 
	" USE Achaldep; "+	
	" INSERT INTO extract_dbo_opus  "+
	" SELECT '' as ID_D28_S28,  "+
	" reseauoperation,   "+
	" num_serie_support AS identification,    "+
	" code_produit_titre AS code_titre,    "+
	" convert(date, convert(datetime, dthr_operation)) AS date24,"+ 
	" IIf(datepart(hh, convert(datetime, dthr_operation))>=4, convert(date, convert(datetime, dthr_operation)), convert(date, convert(datetime, dthr_operation)-1) ) AS date28,   "+
	" LTrim(RTRIM(num_service)) AS voiture_CAP,   "+
	" cast(substring( cast(DTHR_OPERATION as varchar(50)), 11,9) as time) as heure24 ,"+
	" IIf(IsNumeric(RIGHT(CODE_LIGNE,1))=1, Convert(int, CODE_LIGNE), Convert(int, LEFT(CODE_LIGNE,LEN(CODE_LIGNE)-1))) AS lig_cap, "+
	" cast(RIGHT(NoBus,4) as int) AS no_bus_cap,    "+ 
	" IIf( datepart(hh, convert(datetime, dthr_operation))>=4, datepart(hh,convert(datetime, dthr_operation))*3600+datepart(mi, convert(datetime, dthr_operation))*60+datepart(ss,convert(datetime, dthr_operation)), "+
	" datepart(hh,convert(datetime, dthr_operation))*3600+datepart(mi, convert(datetime, dthr_operation))*60+datepart(ss, convert(datetime, dthr_operation))+86400) as seconde28, "+
	" null as type_val,   "+
	" 0 as trajet,   "+
	" 0 as ordre_valid,   "+
	" null as chaine_val,   "+
	" IIF(code_produit_titre<>'GR15',1,6) AS nb_montants ,  "+
	" 0 as ordre_valid_total  "+
	" FROM Opus"+global.ANNEEopus2+".dbo.opus" +
	" WHERE dthr_operation BETWEEN '"+global.DTHRDebut+"' and '"+global.DTHRFin+"' AND ind_support_test='0'";
		
	let extractgfi=
	"USE Achaldep; "+
	" IF OBJECT_ID('extract_gfi', 'U') IS NOT NULL DROP TABLE extract_gfi; CREATE TABLE extract_gfi("+
	"identification	nvarchar(255),"+
	"reseauoperation int,"+
	"code_titre	char(25),"+
	"assignation varchar(100),"+
	"type_service char(2),"+
	"date24	date,"+
	"date28	date NOT NULL,"+
	"heure24 time,"+
	"no_bus_gfi	int,"+
	"ligne_gfi	int,"+
	"voiture_gfi int,"+
	"seconde28	int,"+
	"nb_montants int,"+
	"no_bus	varchar(100),"+
	"voiture varchar(100),"+
	"direction	char(1),"+
	"trace	varchar(100),"+
	"hre_pre_dep varchar(100),"+
	"hre_pre_arr varchar(100),"+
	"pos_course	decimal(7,0),"+
	"source_imput varchar(100) "+
	"PRIMARY KEY (date28, no_bus_gfi, seconde28) with (IGNORE_DUP_KEY = ON));"+	
	"CREATE INDEX date28 ON extract_gfi(date28 ASC);"+
	"CREATE INDEX identification ON extract_gfi(identification ASC);"+
	"CREATE INDEX no_bus_gfi ON extract_gfi(no_bus_gfi ASC);"+
	"CREATE INDEX seconde28	ON extract_gfi(seconde28 ASC);"+
	"CREATE INDEX voiture_gfi ON extract_gfi(voiture_gfi ASC);";
	
	Extraction_gfi = extractgfi +" INSERT INTO extract_gfi SELECT CONCAT('COMPT-', IIf(datepart(hh,a.ts)>=4, convert(date,a.ts), convert(date, "+
	" convert(datetime, cast(a.ts as int)-1) )), '-', "+
	" IIf( datepart(hh, a.ts)>=4, datepart(hh,a.ts)*3600+datepart(mi, a.ts)*60+datepart(ss,a.ts), "+
	" datepart(hh,a.ts)*3600+datepart(mi, a.ts)*60+datepart(ss, a.ts)+86400), "+
	" '-', RIGHT(a.bus,4))  AS identification, "+
	" 33 as reseauoperation, "+
	" 'COMPT' as code_titre, "+
	" '' as assignation, "+
	" '' as type_service, "+ 
	" convert(date, convert(datetime,a.ts)) AS date24, "+
	" IIf( datepart(hh,a.ts)>=4, convert(date, convert(datetime, a.ts)),convert(date,convert(datetime, a.ts)-1)) AS date28, "+
	" cast((substring( cast(cast(ts as datetime2) as varchar(50)), 11, 9)) as time) as heure24,"+
	" RIGHT(a.bus,4) AS no_bus_gfi, "+
	" IIf(CAST(a.route AS INT) > 9999, 9999, CAST(a.route AS INT)) AS ligne_gfi, "+
	" CAST(a.run AS INT) AS voiture_gfi,"+
	" IIf( datepart(hh, a.ts)>=4, datepart(hh,a.ts)*3600+datepart(mi, a.ts)*60+datepart(ss,a.ts), "+
	" datepart(hh,a.ts)*3600+datepart(mi, a.ts)*60+datepart(ss, a.ts)+86400) AS seconde28,  "+
	" IIF(b.amt-CAST(b.amt/"+passage_unitaire+" AS float)*"+passage_unitaire+"<1.65,CAST(b.amt/"+passage_unitaire+" AS INT),CAST(b.amt/"+passage_unitaire+" AS INT)+1) AS nb_montants, "+
	" NULL as no_bus, NULL as voiture, NULL as direction, NULL as trace, NULL as hre_pre_dep, NULL as hre_pre_arr, NULL as pos_course, NULL as source_input"+	
	" FROM GFI.dbo.tr as a INNER JOIN GFI.dbo.trmisc as b ON a.loc_n=b.loc_n AND a.id=b.id AND a.tr_seq=b.tr_seq "+
	" WHERE b.amt>=1.65 AND a.ts BETWEEN convert(datetime,'"+global.DTHRDebut+"', 121) and convert(datetime, '"+global.DTHRFin+"', 121); ";	
	
 	Extraction_sar_bus = ''+
	//" USE AchalDep; IF OBJECT_ID('temp06', 'U') IS NOT NULL DROP TABLE temp06; "+
	" Insert INTO extract_sar_bus  "+ 
	" SELECT CAST(SUBSTRING(sar_date,1,4) + '-' +SUBSTRING(sar_date,5,2) +'-' +SUBSTRING(sar_date,7,2) AS date) AS date_j,  "+
	" LTRIM(RTRIM(voiture)) AS voiture, "+ 
	" hre_dep As hre_dep,  "+ 
	" hre_fin As hre_fin,  "+ 
	" RIGHT(no_bus,4) AS no_bus,  "+ 
	" SUBSTRING(hre_dep,1,2)*3600+SUBSTRING(hre_dep,4,2)*60+SUBSTRING(hre_dep,7,2) AS sec_deb_voiture,  "+ 
	" SUBSTRING(hre_fin,1,2)*3600+SUBSTRING(hre_fin,4,2)*60+SUBSTRING(hre_fin,7,2) AS sec_fin_voiture,  "+ 
	" assignation As assignation, "+ 
	" type_service As type_service, ROW_NUMBER() OVER (partition by voiture order BY voiture) as seq_bus"+ 
	" FROM Stad.stad.sar_bus "+  
	" WHERE sar_date BETWEEN "+ global.DATETxtDebut +" and "+ global.DATETxtFin+" ORDER BY no_bus DESC;", //order by Nobus ajouté
	
	Extraction_ref_arret = "USE AchalDep; INSERT INTO extract_ref_arret "+
	" SELECT distinct a.assignation , a.no_arret, a.coord_x , a.coord_y  "+
	" FROM Stad.stad.ref_arret AS a INNER JOIN calendrier As b ON a.assignation=b.assignation "+
	" WHERE b.date_j ='"+ global.dateDebut +"' ; ";
	
	Extraction_sdap_co = " use achaldep; select * into t_sdap_course from stad.stad.sdap_course "+
	" WHERE sdap_date BETWEEN "+ global.DATETxtDebut+ " AND "+global.DATETxtFin +";"+
	" use achaldep; select * into t_sdap_course_arret from stad.stad.sdap_course_arret  "+
	" WHERE sdap_date BETWEEN "+ global.DATETxtDebut+ " AND "+global.DATETxtFin +";",
	
    Extraction_sdap_course = " use achaldep; INSERT INTO extract_sdap_course "+
	" SELECT CAST(SUBSTRING(a.sdap_date,1,4)+'-'+SUBSTRING(a.sdap_date,5,2)+'-'+SUBSTRING(a.sdap_date,7,2) AS date) AS date_j , "+
	" a.no_bus , a.hre_pre_dep , LTRIM(RTRIM(a.voiture)) AS voiture , "+
	" cast(SUBSTRING(MIN(b.hre_reel_arr),1,2) as int)*3600+cast(SUBSTRING(MIN(b.hre_reel_arr),4,2) as int)*60+cast(SUBSTRING(MIN(b.hre_reel_arr),7,2) as int) AS sec_dep_course_reel, "+
	" cast(SUBSTRING(a.hre_reel_arr,1,2) as int)*3600+ cast(SUBSTRING(a.hre_reel_arr,4,2) as int)*60+ cast(SUBSTRING(a.hre_reel_arr,7,2) as int) AS sec_arr_course_reel "+
	" FROM t_sdap_course as a "+
	" LEFT JOIN t_sdap_course_arret as b ON a.sdap_date=b.sdap_date AND a.voiture=b.voiture AND a.hre_pre_dep=b.hre_pre_dep "+
	" WHERE a.sdap_date BETWEEN "+ global.DATETxtDebut+ " AND  "+global.DATETxtFin +" AND a.ligne<>0  "+
	" GROUP BY cast(SUBSTRING(a.sdap_date,1,4)+'-'+SUBSTRING(a.sdap_date,5,2)+'-'+SUBSTRING(a.sdap_date,7,2) as date), "+
	" a.no_bus , a.hre_pre_dep , LTRIM(RTRIM(a.voiture)), "+
	" cast(SUBSTRING(a.hre_reel_arr,1,2) as int)*3600+ cast(SUBSTRING(a.hre_reel_arr,4,2) as int)*60+ cast(SUBSTRING(a.hre_reel_arr,7,2) as int); "+
	" drop table t_sdap_course;";
	
	Extraction_sdap_course_arret =" USE AchalDep; INSERT INTO extract_sdap_course_arret  "+
	" SELECT assignation as assignation,  "+
	" Cast(SUBSTRING(sdap_date,1,4)+'-'+SUBSTRING(sdap_date,5,2)+'-'+SUBSTRING(sdap_date,7,2) as date) AS date_j,  "+
	" LTRIM(RTRIM(voiture)) AS voiture ,  "+
	" SUBSTRING(hre_reel_arr,1,2)*3600+SUBSTRING(hre_reel_arr,4,2)*60+SUBSTRING(hre_reel_arr,7,2) AS seconde_arr_arret ,  "+
	" SUBSTRING(hre_reel_dep,1,2)*3600+SUBSTRING(hre_reel_dep,4,2)*60+SUBSTRING(hre_reel_dep,7,2) AS seconde_dep_arret,  "+
	" no_arret as no_arret,null as rue_inter, hre_pre_dep as hre_pre_dep, position as position,  "+
	" null as coord_x, null as coord_y  "+
	" FROM Stad.stad.sdap_course_arret "+
	" WHERE sdap_date between "+ global.DATETxtDebut+' AND '+global.DATETxtFin +' AND duree>0; ';
	
	Update_extract_sdap_course_arret= "USE AchalDep; Update extract_sdap_course_arret  SET extract_sdap_course_arret.coord_x = b.coord_x , "+
	" extract_sdap_course_arret.coord_y=b.coord_y  "+
	" FROM extract_sdap_course_arret As a INNER Join extract_ref_arret As b ON a.assignation=b.assignation AND a.no_arret=b.no_arret; ";
	
	Extraction_ref_course= "USE AchalDep; INSERT INTO extract_ref_course "+
	' SELECT distinct a.ligne , a.assignation , a.type_service , a.periode, a.voiture , a.hre_pre_dep , a.hre_pre_arr, a.no_seq_course , '+
	' a.no_voyage , a.direction , a.trace , a.pos_arr , b.date_j '+
	' FROM Stad.stad.ref_course AS a '+
	' INNER JOIN calendrier As b ON a.assignation=b.assignation AND a.type_service=b.type_service2  '+
	" WHERE a.ligne<>0 AND b.date_j ='"+ global.dateDebut+"';";
	
	Extraction_ref_course_arret = 
	" USE AchalDep; INSERT INTO extract_ref_course_arret SELECT distinct "+
	" a.assignation , a.type_service ,a.ligne ,a.voiture ,d.direction  , a.hre_pre_dep , a.no_arret ,a.hre_pre_arret, "+
	" cast(SUBSTRING(a.hre_pre_dep,1,2) as int)*3600 + cast(SUBSTRING(a.hre_pre_dep,4,2) as int)*60 + cast(SUBSTRING(a.hre_pre_dep,7,2) as int)as seconde_hre_pre_dep , "+
	" cast(SUBSTRING(a.hre_pre_arret,1,2) as int)*3600 + cast(SUBSTRING(a.hre_pre_arret,4,2) as int)*60 + cast(SUBSTRING(a.hre_pre_arret,7,2) as int)as seconde_hre_pre_arret , "+
	" a.tmps_par_pre, a.info_acces, b.date_j, a.position, c.coord_x, c.coord_y  FROM ((Stad.stad.ref_course_arret as a  INNER JOIN calendrier as b "+
	" ON a.assignation=b.assignation AND a.type_service=b.type_service2)  INNER JOIN extract_ref_arret as c "+ //j'ai ajouté a.tmps_par_pre (temps parcours prévu)
	" ON a.assignation=c.assignation AND a.no_arret=c.no_arret)  INNER JOIN extract_ref_course as d "+
	" ON a.assignation=d.assignation AND a.voiture=d.voiture AND a.hre_pre_dep=d.hre_pre_dep AND a.type_service=d.type_service  "+
	" WHERE a.ligne<>0 AND b.date_j ='"+ global.dateDebut+"';";

	Extraction_ref_course_arret_suivant= "USE AchalDep; INSERT INTO extract_ref_course_arret_suivant SELECT distinct "+
	" a.assignation , a.type_service , a.ligne , Ltrim(rtrim(a.voiture)) as voiture , b.periode, a.hre_pre_dep ,a.no_arret ,"+
	" a.position, a.coord_x, a.coord_y ,c.hre_pre_arr as hre_pre_arr_suivant ,c.direction as direction_suivant ,c.trace as trace_suivant ,"+
	" c.ligne as ligne_suivant ,c.hre_pre_dep as hre_pre_dep_suivant ,d.no_arret as no_arret_suivant ,d.position AS position_suivant ,d.coord_x AS coord_x_suivant ,d.coord_y AS coord_y_suivant  "+
	" FROM (((extract_ref_course_arret as a INNER JOIN extract_ref_course as b ON a.assignation=b.assignation AND a.ligne=b.ligne AND a.voiture=b.voiture AND a.hre_pre_dep=b.hre_pre_dep AND a.type_service=b.type_service) "+
	" INNER JOIN extract_ref_course as c ON b.assignation=c.assignation AND b.ligne=c.ligne AND b.voiture=c.voiture AND b.no_seq_course=c.no_seq_course-1 AND b.type_service=c.type_service) "+
	" INNER JOIN extract_ref_course_arret as d ON c.assignation=d.assignation AND c.ligne=d.ligne AND c.voiture=d.voiture AND c.hre_pre_dep=d.hre_pre_dep AND c.type_service=d.type_service) "+
	" INNER JOIN calendrier as e ON a.assignation=e.assignation AND a.type_service=e.type_service2 WHERE a.ligne<>0 AND a.info_acces = 'der' AND d.info_acces = 'pre' 	"+
	" AND e.date_j ='"+ global.dateDebut+"'	;";	

	let reqq = [Extraction_sdap_co, Extraction_sdap_course, 
		Alimentation_Calendrier, Extraction_gfi,Extraction_dbo_Opus, Extraction_sar_bus, Extraction_ref_arret, 
		Extraction_sdap_course_arret, Update_extract_sdap_course_arret, Extraction_ref_course,	
		Extraction_ref_course_arret,Extraction_ref_course_arret_suivant   
	];	
	
	let reqqname = ["Extraction_sdap_co","Extraction_sdap_course","Alimentation_Calendrier", "Extraction_gfi","Extraction_dbo_Opus",	
		"Extraction_sar_bus", "Extraction_ref_arret", "Extraction_sdap_course_arret",
		"Update_extract_sdap_course_arret",	"Extraction_ref_course",
		"Extraction_ref_course_arret",	"Extraction_ref_course_arret_suivant"	
	]; 
		
	let t=0, i=1, ech=0, succ=0, j=reqqname.length; // compter echec, success, nd requetes
	let nt=0; //compter combien de fois on a essayé d'importer les données
		sql.close();

	sql.connect(config, function (err){
		console.log('Connecté au serveur de données');
		try {
			function fexec(t){
				new sql.Request().query(reqq[t], function (err, getdata) {				
					if (err) {
						console.log(reqq[t])
						console.log( ftimestamp(new Date())+' - Requête ', reqqname[t], 'échoué ==> Erreur: '+err+') -- ('+i+'/'+j+')');
						fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
							ftimestamp(new Date())+' - Requête: '+ reqqname[t]+'échoue '+'\n'
							,function (err) {
								if (err) console.log(err);
							}
						);	
						testfin();						
					}
					else{ 
						console.log(ftimestamp(new Date())+' - Requête '+ reqqname[t]+' exécutée avec succès -- ('+getdata.rowsAffected[0]+') -- ('+i+'/'+j+')');
						fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
							ftimestamp(new Date())+' - Requête: '+ reqqname[t]+' : exécutée avec succès ('+getdata.rowsAffected[0]+' lignes affectées)\n'
							,function (err) {
								if (err) console.log(err);
							}
						);
						t++; succ++; i++;
						
						if(getdata.rowsAffected[0]==0) testfin();
							
							
						else if(t<reqq.length) fexec(t);
						else 	testfin();
					}
				})
			}fexec(t)	
		} catch(err) { 
			console.log(err);
		}
	})
	
	function testfin(){
		console.log ('Total de requêtes exécutées avec succès :', succ+'/'+j);
		console.log ('Total de requêtes ayant échoué :', ech+'/'+j);
		fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
			ftimestamp('\n'+new Date())+' - Fin du traitement des requêtes (Requetes_SQL) de Mise à jour du calendrier et '+
				'\n\t\t Extraction des données sur la plage de temps \n'+ftimestamp('\n'+new Date())+': - Total de requêtes exécutées avec succès : '+
				succ+'/'+j+'\n'+ftimestamp('\n'+new Date())+' - Total de requêtes ayant échoué : '+ ech+'/'+j,
			function (err) {
				if (err) throw err;
			}
		);
		
		if(succ<j){ 
			console.log(ftimestamp(new Date())+': erreur d\'importation de données'); 
			//Boucle -- Tant que toutes les requetes d'importation n'ont pas 
			//réussi avec succès ré-essayer chaque 30 minutes
			setTimeout( ()=>Requetes_SQL(), 30*60*1000); 
			nt++;
			if(nt>5){
				fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
					ftimestamp(new Date())+' - Toutes les requêtes n\'ayant pas réussi'+'\n'
					,function (err) {
						if (err) console.log(err);
					}
				);
				setTimeout( ()=>lastDate(), 60*60*1000*24); //recommence dans 24 heures après 5 tentatives	
			} 
		} 
		else nbImport(); // Si oui appeler la fonction suivante nbImport() qui vérifiera le nombre d'importations pour chaque table
	}
}
   	
//'Vérification que des données Opus, Comptant, SAR et SDAP ont bien été extraites
//calculer le nombre de lignes insérées
var check='a';  
function nbImport(){
	console.log ('\nVérifier le nombre de lignes de données insérées dans les tables temporaires');
	check='a'; 
	let extract_gfi = "SELECT count(*) as nb_records FROM extract_gfi";
	let extract_dbo_opus = "SELECT count(*) as nb_records FROM extract_dbo_opus";
	let extract_sar_bus = "SELECT count(*) as nb_records FROM extract_sar_bus";
	let extract_sdap_course = "SELECT count(*) as nb_records FROM extract_sdap_course";
	let extract_sdap_course_arret = "SELECT count(*) as nb_records FROM extract_sdap_course_arret";
	
	//liste de requetes SQL
	let mikael=0, listereq = [extract_gfi, extract_dbo_opus, extract_sar_bus,extract_sdap_course, extract_sdap_course_arret],  listereqno = ["extract_gfi", "extract_dbo_opus", "extract_sar_bus", "extract_sdap_course", "extract_sdap_course_arret"];		
	try {
		try{
			let request = new sql.Request();
			for (let  i=0, j=listereq.length; i<j; i++){
				request.query(listereq[i], function (err, getdata) {
					if (err) console.log(err); 
					else{ 
						mikael++;
						console.log (ftimestamp(new Date())+' - '+getdata.recordset[0].nb_records, 'insérées dans '+ listereqno[i]);
						fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 					
							'\n'+ftimestamp(new Date())+': Étape03 - Vérification du nombre de lignes extraites - '+getdata.recordset[0].nb_records+ ' insérées dans '+ listereqno[i]
							,function (err) {
								if (err) throw err;
							}
						);
						if(getdata.recordset[0].nb_records<1) {
							console.log(ftimestamp(new Date())+" - Étape03: Importation de données échouée pour "+listereqno[i]);
							global.check="Échec d'importation de données. Voir le log";
							let tab2del =[ "CAP_GFI_temp", "chainetemp", "chainetemp1","t_sdap_course", "t_sdap_course_arret", "calendrier", "bus_course", 
											"boucle_course", "BD_Valid_Metro", "arrets_probables", "arrets_possibles", "arret_metro", 
											"extract_gfi","textract_gfi02", "extract_dbo_opus", "dico_titre", "dernieres_validations", 
											"chaine_validation", "chaine_validation2", "chaine_validation3", "extract_ref_arret", 
											"extract_ref_course", "extract_ref_course_arret", "extract_ref_course_arret_suivant", 
											"extract_sar_bus", "extract_sar_bus_","extract_sdap_course", "extract_sdap_course_arret", 
											"liste_arret_boucle", "ordonnancement_Valid", "ordonnancement_Valid_2", 
											"pos_course_dans_voiture", "semaine_relache", "validations_successives" ];
							delTable(tab2del);
							setTimeout(()=>{lastDate() }, 1000*60*30) // -- Abort -- arrêt du processus et planifie relancement
						}
						else if(mikael>=listereq.length && check=='a'){
							console.log(ftimestamp(new Date())+' - OpusGFI est appelée pour la fusion des données OPUS et GFI');
							opusGfi();
						}
					}	
				});
			};
		}
		finally {
			//je peux mettre ce que je veux ici
		}
	}
	catch(err) { 
		console.log(err);
	}
};

//'Réunion des données OPUS et GFI
async function opusGfi(){
	//'Restauration de la clé primaire --a rouler apres la boucle forEach précédente (nbImport)
	//Adapter pour éliminer les doublons sur ID_D28_S28 qui doit être unique
	let putkey=	" ALTER TABLE Extract_dbo_Opus ADD CONSTRAINT PrimaryKey PRIMARY KEY (ID_D28_S28) with (IGNORE_DUP_KEY = ON) ;"
		
	let mergeReq = [Reunion_extract_opus_gfi, Cle_ID];
	let j=mergeReq.length;
	let request = new sql.Request();
	try {
		let succ=0;
		function suzuki(){
			function Reunion_extract_opus_gfi(){
				request.query(mergeReq[0], function (err, getdata) {
					if (err) {
						console.log(mergeReq[0]+" : \n"+err);
						fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 					
							'\n'+ftimestamp(new Date())+' - ÉCHEC Réunion des données GFI dans OPUS -- ÉCHEC'
							,function (err) {
								if (err) throw err;
							}
						);
					}
					else{ 
						if(getdata.rowsAffected>=0) {
							console.log (ftimestamp(new Date())+' - Réunion des données OPUS et GFI réussies ('+getdata.rowsAffected[0]+')');
							fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 					
								'\n'+ftimestamp(new Date())+' - Réunion des données GFI dans OPUS réussies ('+getdata.rowsAffected[0]+')'
								,function (err) {
									if (err) throw err;
								}
							);
							cleId();
						}
					}
				});
			}
 			function cleId(){
				request.query(mergeReq[1], function (err, getdata) { //set cle_ID
					if (err) {
						console.log(mergeReq[1]+" : \n"+err);
						fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 					
							'\n'+ftimestamp(new Date())+' -  Restauration index et contraintes sur Extract_dbo_Opus -- ÉCHEC'
							,function (err) {
								if (err) throw err;
							}
						);
					}
					else{ 
						if(getdata.rowsAffected[0]>=0) {
							console.log (ftimestamp(new Date())+' - Restauration index et contraintes sur Extract_dbo_Opus réussie ('+getdata.rowsAffected[0]+')');
							fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 					
								'\n'+ftimestamp(new Date())+' - Restauration index et contraintes sur Extract_dbo_Opus réussie'
								,function (err) {
									if (err) throw err;
								}
							);
							//indexOn();
							numerotation(); 
						}
					}
				});
			} 
			Reunion_extract_opus_gfi();
		}
		await suzuki();
		function indexOn(){ 
			request.query(putkey, function (err, getdata) {
				if(err) {console.log('Echec :'+ putkey +'\n'+err)}
				else {
					console.log (ftimestamp(new Date())+' - Restauration index et contraintes sur Extract_dbo_Opus réussie');	
					numerotation(); 
				}
			})
		}
	} catch(err) { 
		console.log(err);
	};
};

//Function permttant de générer une confirmation pour les chaines envoyées par le workflow
function confirmation(a, reqqno, getdata){
	console.log (ftimestamp(new Date())+' - ' +reqqno[a]+' exécutée avec succès ('+getdata.rowsAffected[0]+' lignes affectées)');
	fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
		'\n'+ftimestamp(new Date())+' - ' +reqqno[a]+' exécutée avec succès ('+getdata.rowsAffected[0]+' lignes affectées)' 
		,function (err) {
			if (err) throw err;
		}
	)
}

//Numérotation des validations par ordre chronologique (RTL ou métro)
function numerotation(){	
	console.log('\n'+ftimestamp(new Date())+' - Entamer le classement chronologique des validations par usager' );
	console.log (ftimestamp(new Date())+' - Démarrage de la Numérotation des validations par ordre chronologique pour chaque usager');
	let reqq=[Numerotation_toutes_valid, Update_Numerotation_toutes_valid], reqqno=["Numerotation_toutes_valid", "Update_Numerotation_toutes_valid"], i=0;
	
	function worker(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ 
				console.log(reqqno[i]);  
				throw err;  
			}
			else { 
				confirmation(i, reqqno, getdata);
				i++;
				if(i>=reqq.length){ 
					console.log (ftimestamp(new Date())+' - Classement chronologique des validations par usager exécuté avec succès');
					boucle();
				}else worker(i);	
			}	
		});
	}worker(i);
};

//'Boucle détermination des correspondances sur le réseau RTL
//'Nouvel appel des requêtes dans la boucle pour mettre à jour la variable Trajet
async function boucle(){	
	console.log (ftimestamp(new Date())+' - Démarrage de la fonction Boucle de détermination des correspondances sur le réseau RTL');

	function z100(){
		let reqq= [Alimentation_Ordo_Valid, Attribution_1ere_valid], reqqno= ["Alimentation_Ordo_Valid", "Attribution_1ere_valid"], i=0;
		worker(i);
		function worker(i){
			new sql.Request().query(reqq[i], function (err, getdata) {
				if (err){ 
					console.log(reqqno[i]);  
					throw err; 
				}
				else { 
					confirmation(i, reqqno, getdata);
					i++;
					if(i>1){ 
						console.log (ftimestamp(new Date())+' - Détermination des premieres validations pour chaque usager terminée avec succès\n Attributions des caractéristiques type_valid=Bj1 et trajet=1 \n à la 1ère validation de la journée de chaque usager');
						//Appeler la fonction z200() qui traite les validations successives (Bjc)
						z200();
					} else worker(i);	
				}	
			});
		}
	}	
	z100();
	
	async function z200(){
		console.log("\nCall z200()");
		let ordre_validation = 1, req2 = " Select count(identification) as avance From ordonnancement_Valid where type_val IS NULL; ";	
		let round=1; //compter combien de fois la boucle a été exécutée
		let avancement=0,ki=0; 
		
		// Au moins sera exécuté une fois 
		function bouclet(){
			//Lancer les requêtes successivement
			//console.log("Let's go there");		
			Alimentation_ordo_Valid_2 =  " IF OBJECT_ID('dbo.ordonnancement_Valid_2', 'U') IS NOT NULL  DROP TABLE ordonnancement_Valid_2; "+
				" SELECT identification , date28 , MAX(trajet) as trajet "+
				" INTO ordonnancement_Valid_2  "+
				" FROM ordonnancement_Valid GROUP BY identification, date28 ;";

			Update_ordo_Valid_1= "	UPDATE a "+
				" SET a.type_val='Bjc', a.trajet=b.trajet "+
				" FROM ordonnancement_Valid as a "+
				" INNER JOIN ordonnancement_Valid as b ON a.identification=b.identification AND a.date28=b.date28 AND a.lig_cap<>b.lig_cap AND a.seconde28 <= b.seconde28+5400 "+
				" INNER JOIN ordonnancement_Valid_2 c ON b.identification=c.identification AND b.date28=c.date28 AND b.trajet=c.trajet "+
				" where a.ordre_valid= " + ordre_validation + "  AND  b.type_val='Bj1'; ";

			Update_ordo_Valid_2= " UPDATE a "+ 
				" SET a.type_val='Bj1' , a.trajet=b.trajet+1   "+
				" FROM ordonnancement_Valid  as a"+
				" INNER JOIN ordonnancement_Valid as b ON a.identification=b.identification AND a.date28=b.date28 AND a.seconde28<>b.seconde28 AND a.lig_cap=b.lig_cap AND a.trajet = b.trajet  "+
				" WHERE a.ordre_valid=" + ordre_validation + "  AND b.type_val='Bjc';  ";

			Update_ordo_Valid_3 = "	UPDATE a "+
				" SET a.type_val='Bj1' , a.trajet=b.trajet+1  "+
				" FROM ordonnancement_Valid as a INNER JOIN ordonnancement_Valid_2 as b ON a.identification=b.identification AND a.date28=b.date28 "+
				" WHERE a.ordre_valid=" + ordre_validation + "  AND a.type_val IS NULL;	";
				
			let i=0, reqq = [Alimentation_ordo_Valid_2, Update_ordo_Valid_1 , Update_ordo_Valid_2, Update_ordo_Valid_3],
			reqqno = ["Alimentation_ordo_Valid_2", "Update_ordo_Valid_1", "Update_ordo_Valid_2", "Update_ordo_Valid_3"];
				
			function worker(i){
				new sql.Request().query(reqq[i], function (err, getdata) {
					if (err){ 
						console.log(reqqno[i]);  
						throw err; 
					}
					else { 
						confirmation(i, reqqno, getdata);
						i++;
						if(i>=reqq.length){ 
							console.log (ftimestamp(new Date())+' - Détermination des premieres validations pour chaque usager terminée avec succès\n'+
							'Attributions des caractéristiques type_valid=Bj1 et trajet=1 \n à la 1ère validation de la journée de chaque usager');
							//Appeler la fonction avancoo() qui calcule la valeur avance et refait la boucle si nécessaire
							avancoo();
						}else worker(i);	
					}	
				});
			}worker(i)
			
			function avancoo(){
				console.log('ordre_validation :', ordre_validation);
				new sql.Request().query(req2, function (err, getdata) {
					if (err) console.log('Erreur :', req2 +'\n'+ err);
					avancement=getdata.recordset[0].avance;
					round++;
					ordre_validation++;
					console.log('avancement : ', avancement);
					if(avancement>0) bouclet(); else boucletest();
				});
			}
		}
		bouclet();
		
		function boucletest(){
			console.log('test 2 avancement = ', avancement);
			console.log('round', round);
			//Après la boucle, appeler la fonction z300() pour mettre à jour la colonne type_val dans extract_dbo_opus
			if(avancement==0 && round !=1){
				console.log (ftimestamp(new Date())+' - Validations successives identifiées / Boucle  complétée - avec succès');
				z300();
			} 
			else bouclet();
		}
	}
	
	//Après la boucle précédente, mettre à jour la table extract_dbo_valid_avec_correspondance
	function z300(){
		console.log (ftimestamp(new Date())+' - Démarrage de la mise à jour de la table des validations  z300()');
		new sql.Request().query(Update_extract_dbo_Valid_avec_correspondance, function (err, getdata) {
			if (err) console.log(err);
			else { 	
				console.log (ftimestamp(new Date())+' - Update_extract_dbo_Valid_avec_correspondance exécutée avec succès ('+getdata.rowsAffected[0]+' lignes affectées)');
				if(getdata.rowsAffected[0]>=0){
					chaineValidation();
				}
			}	
		});	
	}
}
 
//'Enchainement de requêtes de détermination des chaînes de validations (série de validations faites par un groupe de personnes montant en même temps au même arrêt)
	function chaineValidation(){ 
	console.log (ftimestamp(new Date())+' - ChaineValidation() est appelée');
	let reqq=[ALIMENTATION_CHAINE_VALIDATION,ORDONNANCEMENT_VALIDATION_COURSE,CREATION_CHAINE_VALIDATION_2,CREATION_CHAINE_VALIDATION_3, UPDATE_extract_dbo_opus_AVEC_CHAINE_VALID1,UPDATE_extract_dbo_opus_AVEC_CHAINE_VALID2],
	reqqno=["ALIMENTATION_CHAINE_VALIDATION","ORDONNANCEMENT_VALIDATION_COURSE", "CREATION_CHAINE_VALIDATION_2","CREATION_CHAINE_VALIDATION_3", "UPDATE_extract_dbo_opus_AVEC_CHAINE_VALID1","UPDATE_extract_dbo_opus_AVEC_CHAINE_VALID2"], i=0;	
	function worker(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ 
				console.log(reqqno[i]);  
				throw err; 
			}
			else { 
				confirmation(i, reqqno, getdata);
				i++;
				if(i>=reqq.length){ //
					console.log( ftimestamp(new Date())+' - Enchainement de requêtes de détermination des chaînes de validation (série de validations faites par un groupe de personnes montant en même temps au même arrêt) ');
					alimentation_CP_GFI_et_Valid_Metro();
				}else worker(i);	
			}	
		});
	}worker(i)	
}

//'Alimentation du fichier CAP_GFI
async function alimentation_CP_GFI_et_Valid_Metro(){  
	let reqq=[Alimentation_CAP_GFI, Alimentation_BD_VALID_METRO];
	let reqqno=["Alimentation_CAP_GFI", "Alimentation_BD_VALID_METRO"];
	
	function alimentationCapGFI(){
		new sql.Request().query(reqq[0], function (err, getdata) {
			if (err) console.log(err); 
			else { 
				confirmation(0, reqqno, getdata);
				if(getdata.rowsAffected[0]>=0){
					alimentationValidMetro();	
				}
			}	
		});
	}
	function alimentationValidMetro(){
		new sql.Request().query(reqq[1], function (err, getdata) {
			if (err) console.log(err); 
			else { 
				console.log ("\nGénération des tables bus_course et CAP_GFI_temp avec succès");
				confirmation(1, reqqno, getdata);
				pzoppi();
			}	
		});
	}
	alimentationCapGFI();	
	
	function pzoppi(){
		fichier_bus_course();
		fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
			'\n'+ftimestamp('\n'+new Date())+" Génération des tables bus_course et CAP_GFI_temp avec succès"
			,function (err) {
				if (err) throw err;
			}
		);
	}
}

//'Création du fichier bus_course (qui réunit les données de répartition des bus, des courses prévus (Hastus) et des courses réalisées (sdap)
function fichier_bus_course(){  
	Position_course_dans_voiture = 	"INSERT INTO pos_course_dans_voiture SELECT b.date_j as date_j, "+
	 " a.type_service AS type_service, "+
	 " a.voiture AS voiture, "+
	 " MIN(a.no_seq_course) AS pre_course, "+
	 " MAX(a.no_seq_course) AS der_course, "+
	 " b.no_bus as no_bus, "+
	 " a.assignation AS assignation,  "+
	 " MIN(a.hre_pre_dep) AS hre_pre_dep_pre ,"+
	 " MAX(a.hre_pre_arr) AS hre_pre_arr_der  "+
	 " FROM extract_ref_course AS a LEFT JOIN extract_sar_bus as b ON a.voiture=b.voiture AND a.hre_pre_dep>=b.hre_dep AND a.hre_pre_arr<=b.hre_fin  "+
	 " WHERE b.date_j BETWEEN '"+global.dateDebut+"' AND '"+global.dateFin+"' GROUP BY b.date_j, a.assignation, a.type_service, a.voiture, b.no_bus; ";

	let reqq=[Position_course_dans_voiture, Alimentation_bus_course, correctionWDK, Update_bus_course_avec_extract_sar_bus, Update_bus_course_avec_extract_sdap_course], i=0,
	reqqno=["Position_course_dans_voiture", "Alimentation_bus_course", "correctionWDK", "Update_bus_course_avec_extract_sar_bus", "Update_bus_course_avec_extract_sdap_course"];
	function worker(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ 
				console.log(reqq[i]);  
				throw err; 
			}
			else { 
				confirmation(i, reqqno, getdata);
				i++;
				if(i>=reqq.length){ 
					console.log( ftimestamp(new Date())+' - Fin de la création du fichier bus_course ');
					mise_en_relation_CAP_GFI_Bus_Course();	
				}else worker(i);	
			}	
		});
	}worker(i)	
}		
		
//'Enchainement des requêtes de mise en relation entre CAP_GFI et bus_course
function mise_en_relation_CAP_GFI_Bus_Course(){
	console.log('\n'+ftimestamp(new Date())+' - Enchainement des requêtes de mise en relation entre CAP_GFI et bus_course');
	let reqq =[MATCH1,MATCH2,MATCH3,MATCH5,MATCH6,MATCH7,MATCH9,MATCH10,MATCH11,MATCH13,MATCH14,MATCH15, MATCH4,MATCH8,MATCH12,MATCH16,MATCH17,MATCH18,MATCH19,MATCH20,MATCH21,MATCH22,MATCH23,MATCH24,MATCH25];	
	let reqqno =["MATCH1","MATCH2","MATCH3","MATCH5","MATCH6","MATCH7","MATCH9","MATCH10","MATCH11","MATCH13","MATCH14","MATCH15","MATCH4","MATCH8","MATCH12","MATCH16","MATCH17","MATCH18","MATCH19","MATCH20","MATCH21","MATCH22","MATCH23","MATCH24","MATCH25"];	
	
	let i=0;
	matches(i);
	function  matches(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ 
				console.log(reqqno[i]);  
				throw err; 
			}
			else { 
				confirmation(i, reqqno, getdata);
				if(getdata.rowsAffected[0]>=0){
					i++;
					if(i<reqq.length) matches(i);	
					else { 
						console.log( ftimestamp(new Date())+' - Mise en relation CAP_GFI et Bus_Course complétée avec succès ');
						index_histo_emb();
					}
				}
			}	
		});
	}
}		

//'---------------------------------------------------
//'ATTRIBUTION D'ARRÊT D'EMBARQUEMENT AUX VALIDATIONS	
//'---------------------------------------------------
function index_histo_emb(){	
	let add_index= "CREATE INDEX index_histo_emb ON CAP_GFI_temp (identification,ligne,direction)" ;
	new sql.Request().query(add_index, function (err, getdata) {
		if (err) console.log(err); 
		else { 
			let f01=ftimestamp(new Date());
			console.log(ftimestamp(new Date())+" - Ajout index sur CAP_GFI_temp avec succès");
			//Enregistrement dans le log
			fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
				"\n Ajout index sur CAP_GFI_temp avec succès: "+ftimestamp(new Date())+'\n'
				,function (err) {
					if (err) throw err;
				}
			);	
			attribution_emb_ligne45();
		}	
	});
}

//'ATTRIBUTION DES ARRÊTS DE D EMBARQUEMENT AUX VALIDATIONS SUR LA LIGNE 45 
//(de fonction de la direction de la course il n'existe qu'une possibilité d'embarquement et de débarquement)
function attribution_emb_ligne45(){
	new sql.Request().query(ATTRIBUTION_EMB_L45, function(err, getdata) {
		if (err) console.log(err); 
		else { 
			if(getdata.rowsAffected[0]>=0){
				console.log (ftimestamp(new Date())+' - Attribution des arrêts de d\'embarquement L.45 avec succès ('+getdata.rowsAffected[0]+' lignes)');
				fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
					'\n'+ftimestamp(new Date())+' - Attribution des arrêts de \embarquement L.45 avec succès --('+getdata.rowsAffected[0]+' lignes)'
					,function (err) {
						if (err) throw err;
					}
				);
				attribution_arret_sdap();
			}
		}	
	});	
}

//'ATTRIBUTION_ARRET_SDAP selon l'heure réelle de passage à l'arrêt
function attribution_arret_sdap(){
	let extract_sdap= " use achaldep; IF OBJECT_ID('extract_sdap', 'U') IS NOT NULL DROP TABLE extract_sdap; "+
	" select * into extract_sdap from stad.stad.sdap_course_arret where sdap_date in ("+ global.DATETxtDebut +", "+ global.DATETxtFin +");" +
	" Update a SET a.coord_x_reel = b.coord_x , a.coord_y_reel=b.coord_y "+
	" FROM extract_sdap As a INNER Join extract_ref_arret As b ON a.assignation=b.assignation AND a.no_arret=b.no_arret ;" ;
	
	let ATTRIBUTION_ARRET_SDAP= "UPDATE a " +
	" SET a.no_arret_emb=b.no_arret , a.imput_emb=2, a.position_arret_emb=b.position, a.coord_x_emb = b.coord_x_reel, a.coord_y_emb = b.coord_y_reel " +
	" From CAP_GFI_temp as a INNER JOIN extract_sdap as b on "+
	" a.date24=Convert(date, concat(SUBSTRING(b.sdap_date,1,4),'-',SUBSTRING(b.sdap_date,5,2),'-',SUBSTRING(b.sdap_date,7,2)), 121) "+
	" and a.ligne=b.ligne and a.voiture=b.voiture where datepart(HH,a.heure24)*3600+datepart(MINUTE,a.heure24)*60+datepart(SECOND,a.heure24) "+
	" between SUBSTRING(b.hre_reel_arr,1,2)*3600+SUBSTRING(b.hre_reel_arr,4,2)*60+SUBSTRING(b.hre_reel_arr,7,2) "+
	" and SUBSTRING(b.hre_reel_dep,1,2)*3600+SUBSTRING(b.hre_reel_dep,4,2)*60+SUBSTRING(b.hre_reel_dep,7,2)+15 "; // paramétrable ici -- plus 15 secondes
	
	//La liaison sdap est ligne, voiture, date, heeure 
	//une voiture sur une ligne donnée est à une et une seule position pendant une seconde donnée
	//seconde de validation est entre seconde arrivée  et seconde départ de l'arrêt 
	let ATTRIBUTION_ARRET_SDAP_KT = " UPDATE CAP_GFI_temp  SET CAP_GFI_temp.no_arret_emb=b.no_arret , CAP_GFI_temp.imput_emb=2, "+
		" CAP_GFI_temp.position_arret_emb=b.position, CAP_GFI_temp.coord_x_emb = b.coord_x_reel, CAP_GFI_temp.coord_y_emb = b.coord_y_reel  "+
		" From CAP_GFI_temp as a INNER JOIN [dbo].[extract_sdap] as b ON "+
		" a.date24=CAST(SUBSTRING(b.sdap_date,1,4)+'-'+SUBSTRING(b.sdap_date,5,2)+'-'+SUBSTRING(b.sdap_date,7,2) AS date)  "+
		" AND a.voiture=b.voiture AND a.ligne=b.ligne AND a.seconde28 BETWEEN SUBSTRING(b.hre_reel_arr,1,2)*3600+SUBSTRING(b.hre_reel_arr,4,2)*60+SUBSTRING(b.hre_reel_arr,7,2) and   "+
		" SUBSTRING(b.hre_reel_dep,1,2)*3600+SUBSTRING(b.hre_reel_dep,4,2)*60+SUBSTRING(b.hre_reel_dep,7,2)+15 WHERE a.no_arret_emb is null; ";// paramétrable ici -- plus 15 secondes
		
	let reqq=[extract_sdap,ATTRIBUTION_ARRET_SDAP,ATTRIBUTION_ARRET_SDAP_KT], reqqno=["extract_sdap","ATTRIBUTION_ARRET_SDAP", "ATTRIBUTION_ARRET_SDAP_KT"], i=0;
	worker(i);
	function worker(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ 
				console.log(reqq[i]);  
				throw err; 
			}
			else { 
				confirmation(i, reqqno, getdata);
				i++;
				if(i>reqq.length){ 
					console.log(ftimestamp(new Date())+" - Attribution d'arrêts d'embarquement SDAP exécutée avec succès ("+getdata.rowsAffected[0]+' lignes)');
					fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
						"\n Attribution d'arrêts d'embarquement SDAP- avec succès -"+ftimestamp(new Date())+'\n'
						,function (err) {
							if (err) throw err;
						}
					);
					attribution_arret_stad1();
				}else worker(i);	
			}	
		});
	}	
}   

//' ATTRIBUTION_ARRET_STAD_1 (sélection des arrêts possible selon l'heure prévue avec buffer de 40 secondes, 
//puis détermination de l'arrêt le plus probable)
function attribution_arret_stad1(){
	let reqq=[Determination_embarquement_possible, Determination_embarquement_probable, UPDATE_CAP_GFI_embarq_stad, Vidange_table_temporaire_17, Vidange_table_temporaire_18];	
	let reqqno=[ "Determination_embarquement_possible", "Determination_embarquement_probable","UPDATE_CAP_GFI_embarq_stad", "Vidange_table_temporaire_17", "Vidange_table_temporaire_18"];	
	let i=0;
	worker(i);
	function worker(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ 
				console.log(reqqno[i]);  
				throw err; 
			} 
			else { 
				confirmation(i, reqqno, getdata);
				i++;
				if(i>=reqq.length){ 
					console.log( ftimestamp(new Date())+' - Les Attributions d\'Arret_Stad1 complétées avec succès (N/A lignes)');
					attribution_arret_stad_2();
				}else worker(i);	
			}	
		});
	}	
}  

function attribution_arret_stad_2(){
	let reqq=[ATTRIBUTION_ARRET_STAD_2,ATTRIBUTION_ARRET_STAD_3, ATTRIBUTION_ARRET_STAD_4],
	reqqno=[ "ATTRIBUTION_ARRET_STAD_2", "ATTRIBUTION_ARRET_STAD_3","ATTRIBUTION_ARRET_STAD_4"],	
	i=0;
	worker(i);
	function worker(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ console.log(reqqno[i]);  
				throw err; 
			}
			else { 
				confirmation(i, reqqno, getdata);
				i++;
				if(i>=reqq.length){ 
					console.log(ftimestamp(new Date()) +" - Attribution d'arrêts d'embarquement STAD2 exécutée avec succès--("+getdata.rowsAffected[0]+' lignes)');
					fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
						"\n Attribution d'arrêts d'embarquement STAD2 - avec succès -"+ftimestamp(new Date())+'\n'
						,function (err) {if (err) throw err;}
					);
					historique_arret_emb();
				}else worker(i);	
			}	
		});
	}		
} 

//' ATTRIBUTION_ARRET_HISTORIQUE -- embarquements (selon les habitudes des usagers)
async function historique_arret_emb(){
	console.log(ftimestamp(new Date())+" - Début historique_arret_emb");
	let anneeopus4a='', vv=global.ANNEEopus4;
	let mm=new Date((+global.DernierJourTraite) + 86400000*1).getMonth(), 
	jj=new Date((+global.DernierJourTraite) + 86400000*1).getDate() ;
	
	//est-ce premier janvier
	if(mm==0 && jj==1) {
		//considérer le mois de décembre de l'année précédente
		let Cap_gfi_dec21 = " use achaldep; IF OBJECT_ID('cap_gfi_1201', 'U') IS NOT NULL DROP TABLE cap_gfi_1201; "+
			" select top (1) * into cap_gfi_1201 from cap_gfi_"+(global.ANNEEopus4-1)+
			"; truncate table cap_gfi_1201; "+
			" insert into cap_gfi_1201 select * from cap_gfi_"+(global.ANNEEopus4-1)+
			" where date28>'"+(global.ANNEEopus4-1)+"-12-10'; "
			//"alter table cap_gfi_1201 add categorie varchar(100);"
			
		
		function histo1201(){		
			new sql.Request().query(Cap_gfi_dec21, function(err, getdata) {
				if (err) {
					console.log(Cap_gfi_dec21); 
					console.log(err); 
				}else { 
					console.log(ftimestamp(new Date())+' - Données historiques 21 derniers jours de décembre - exécutée avec succès ('+getdata.rowsAffected[0]+' lignes)');
					fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
						"\n "+ftimestamp(new Date())+ ' - Préparation ou mise à jour données historiques 21 jours de decembre('+getdata.rowsAffected[0]+' lignes)'
						,function (err) {if (err) throw err;}
					);
					hist100();
				}
			});	
		}
		histo1201(); //créer la table historique des données de decembre
	} else hist100(); //Si pas premier janvier passer directmeent à hist100
	
	function hist100() {
		//Si Janvier on a besoin des données de Decembre de l'année dernière 
		//(on les importe dans cap_gfi_1201 si premier janvier)
		if(mm==0) vv='1201';
		
		Determination_historique = " INSERT INTO arrets_possibles SELECT "+
		" a.ID_D28_S28 as [ID_D28_S28], b.identification as [identification] ,null as [no_arret], "+
		" b.no_arret_emb as [no_arret_emb], null as [no_arret_deb], null as [direction] "+
		" ,null as [position],null as [position_boucle], b.position_arret_emb as [position_arret_emb], "+
		" null as [coord_x], b.coord_x_emb as [coord_x_emb], null as [coord_y], b.coord_y_emb as [coord_y_emb], "+
		" COUNT(*) AS [nombre_occ], null as [proximite_temporelle], null as [proximite_arret] "+
		" FROM CAP_GFI_temp AS a INNER JOIN [dbo].[CAP_GFI_"+vv +
		"] AS b ON a.identification=b.identification AND cast(a.date28 as datetime)-21 <= b.date28 AND a.date28 > b.date28  "+ //parametrable nombre de jours historiques
		" AND a.seconde28-600 <= b.seconde28 AND a.seconde28+600 >= b.seconde28 AND a.ligne=b.ligne AND a.direction=b.direction "+ //parametrable temporel ici +5 et -5 min
		" INNER JOIN extract_ref_course_arret as c ON (a.voiture=c.voiture or a.voiture_CAP=c.voiture) AND a.hre_pre_dep=c.hre_pre_dep AND b.no_arret_emb=c.no_arret  "+
		" WHERE b.imput_emb=2 AND a.imput_emb<2 GROUP BY a.ID_D28_S28 , b.identification , b.no_arret_emb , b.position_arret_emb , b.coord_x_emb , b.coord_y_emb;  ";

		new sql.Request()
		.query(Determination_historique, function(err, getdata) {
			if (err) console.log(err); 
			else { 
				console.log(ftimestamp(new Date())+" - Attribution d'arrêts d'embarquement HISTORIQUE exécutée avec succès ("+getdata.rowsAffected[0]+' lignes)');
				fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
					"\n"+ftimestamp(new Date())+" - Attribution d'arrêts d'embarquement HISTORIQUE exécutée avec succès ("+getdata.rowsAffected[0]+' lignes)'
					,function (err) {if (err) throw err;}
				);
				hist200();
			}
		})	
	}
	
	function hist200(){
		let reqq=[Determination_meilleur_historique, UPDATE_CAP_GFI_embarq_histo, Vidange_table_temporaire_17, Vidange_table_temporaire_18];
		let reqqno=["Determination_meilleur_historique", "UPDATE_CAP_GFI_embarq_histo", "Vidange_table_temporaire_17", "Vidange_table_temporaire_18"]
		let i=0;
		console.log(ftimestamp(new Date())+' - Hist200 est appelée')
		function worker(i){
			new sql.Request().query(reqq[i], function (err, getdata) {
				if (err){ 
					console.log(reqqno[i]);  
					throw err; 
				}
				else { 
					confirmation(i, reqqno, getdata);
					i++;
					if(i>=reqq.length){ 
						console.log(ftimestamp(new Date())+' - ATTRIBUTION_ARRET_HISTORIQUE embarquements (selon les habitudes des usagers) complétée avec succès ');
						attributions_arret_chainage();
					}else worker(i);	
				}
			});
		}worker(i);	
	}
}

//' ATTRIBUTION_ARRET_CHAINAGE (selon les enchaînements de montée de voyageurs)
function attributions_arret_chainage(){
	let reqq=[ATTRIBUTION_ARRET_CHAINAGE, ATTRIBUTION_ARRET_CHAINAGE_STAD , CORRECTION_VALID_DER_ARRET /*,correction_emb, correction_emb_cap, correction_emb_sdap, correction_emb_sdap_cap*/],
	reqqno=["ATTRIBUTION_ARRET_CHAINAGE", "ATTRIBUTION_ARRET_CHAINAGE_STAD", "CORRECTION_VALID_DER_ARRET" /*, "correction_emb", "correction_emb_cap", "correction_emb_sdap", "correction_emb_sdap_cap"*/],
	i=0;
	function worker(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ 
				console.log(reqqno[i]);  
				throw err; 
			}
			else { 
				confirmation(i, reqqno, getdata);
				if(getdata.rowsAffected[0]>=0){
					if(i<reqq.length) {
						//Enregistrement dans le log
						fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
							reqq[i]+ " -- ATTRIBUTION_ARRET_CHAINAGE (selon les enchaînements de montée de voyageurs) complétée avec succès - "+ftimestamp(new Date())+'\n'
							,function (err) {if (err) throw err;}
						);						
					}
					i++;
					if(i>=reqq.length){ 
						console.log(ftimestamp(new Date())+" - ATTRIBUTION_ARRET_CHAINAGE (selon les enchaînements de montée de voyageurs) complétée avec succès ("+getdata.rowsAffected[0]+' lignes)');
						index_histo_deb_2();
					}else worker(i);	
				}
			}	
		});
	} worker(i);		
}

//'-----------------------------------------------------
//'ATTRIBUTION D'ARRÊT DE DEBARQUEMENT AUX VALIDATIONS
//'-----------------------------------------------------

//'Création de l'index index_histo_deb
function index_histo_deb_2(){	
	let add_index= "CREATE INDEX index_histo_deb ON CAP_GFI_temp (identification,no_arret_emb,ligne,direction)" ;
	new sql.Request().query(add_index, function (err, getdata) {
		if (err) console.log(err); 
		else { 
			console.log('\n\n'+ftimestamp(new Date())+" - Création de l'index index_histo_deb -- Ajout index sur CAP_GFI_temp avec succès ("+getdata.rowsAffected[0]+' lignes)');
		
			fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
				"Création de l'index index_histo_deb -- Ajout index sur CAP_GFI_temp -- avec succès: "+ftimestamp(new Date())+'\n'
				,function (err) {if (err) throw err;}
			);
			attribution_deb_ligne45();
		}	
	});
}

//' ATTRIBUTION DES ARRÊTS DE DÉBARQUEMENT AUX VALIDATIONS SUR LA LIGNE 45 (de fonction de la direction de la course 
//il n'existe qu'une possibilité d'embarquement et de débarquement
function attribution_deb_ligne45(){
	new sql.Request().query(ATTRIBUTION_DEB_L45, function(err, getdata) {
		if (err) console.log(err); 
		else { 
			console.log (ftimestamp(new Date())+' - Attribution des arrêts de débarquement L.45 avec succès --('+getdata.rowsAffected[0]+' lignes)');
			fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
				ftimestamp(new Date())+' - Attribution des arrêtes de débarquement L.45 -- avec succès\n'
				,function (err) {
					if (err) throw err;
				}
			);
			ligne_de_fuite();
		}	
	});	
}

//'Définition de la "ligne de fuite" (arrêts restants à parcourir au moment de l'embarquement)
function ligne_de_fuite(){
	let reqq=[ALIMENTATION_BOUCLE_COURSE, ALIMENTATION_LISTE_ARRET_BOUCLE, UPDATE_LISTE_ARRET_BOUCLE, UPDATE_INDICE_ARRETS_PORTES_FERMEES],
	reqqno=["ALIMENTATION_BOUCLE_COURSE", "ALIMENTATION_LISTE_ARRET_BOUCLE", "UPDATE_LISTE_ARRET_BOUCLE", "UPDATE_INDICE_ARRETS_PORTES_FERMEES"],
	i=0;
	function worker(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ console.log(reqqno[i]); throw err; }
			else { 
				confirmation(i, reqqno, getdata);
				i++;
				if(i>=reqq.length){ 
					console.log(ftimestamp(new Date())+" - Ligne de fuite >>> Arrêts restants à parcourir à partir de l'embarquement -- complétée avec succès ("+getdata.rowsAffected[0]+' lignes)');
					debarquements_metros();
				}else worker(i);	
			}
		});
	}worker(i); 
}
	
//'Attribution des débarquements à proximité des stations de métro
async function debarquements_metros(){
	//'écriture des arrêts de débarquement sur la table CAP_GFI
	console.log(ftimestamp('\n\n '+new Date())+' - Début Attribution des débarquements à proximité des stations de métro');
	
	function dmetro100(){
		let reqq=[ALIMENTATION_validations_successives_metro,Debarquements_possibles_metro_hre_reel_pre, Determination_debarquement_probable],
		reqqno=["ALIMENTATION_validations_successives_metro","Debarquements_possibles_metro_hre_reel_pre", "Determination_debarquement_probable"]
		let i=0; 
		function worker(i){
			new sql.Request().query(reqq[i], function (err, getdata) {
				if (err){console.log(reqqno[i]); throw err; }
				else { 
					confirmation(i, reqqno, getdata);
					i++;
					if(i>=reqq.length){ 
						 console.log(ftimestamp(new Date())+' - Débarquements métro complétée avec succès ');
						 global.Code_imputation_debarquement = 1; dmetro200();
					}else worker(i);	
				}
			});
		}worker(i);
	}
	
	//then update_CAP_GFI_debarq
	function dmetro200(){
		UPDATE_CAP_GFI_debarq = " UPDATE CAP_GFI_temp "+
			" SET CAP_GFI_temp.no_arret_deb=z.no_arret_deb , CAP_GFI_temp.imput_deb=  " + global.Code_imputation_debarquement +
			" FROM CAP_GFI_temp as x INNER JOIN (SELECT a.ID_D28_S28 ,a.no_arret_deb FROM (SELECT aa.ID_D28_s28 ,bb.no_arret_deb ,aa.proximite_arret , "+
			" bb.position_boucle ,bb.direction ,bb.coord_x ,bb.coord_y FROM arrets_probables as aa  "+
			" INNER JOIN arrets_possibles as bb "+
			" ON aa.ID_D28_s28= bb.ID_D28_s28 AND aa.direction=bb.direction AND aa.proximite_arret=bb.proximite_arret) as a  "+
			" LEFT JOIN (SELECT cc.ID_D28_s28 ,dd.no_arret_deb ,cc.proximite_arret ,dd.position_boucle ,dd.direction ,dd.coord_x ,dd.coord_y FROM arrets_probables as cc   "+
			" INNER JOIN arrets_possibles as dd   "+
			" ON cc.ID_D28_s28= dd.ID_D28_s28 AND cc.direction=dd.direction AND cc.proximite_arret=dd.proximite_arret) as b   "+
			" ON a.ID_D28_S28=b.ID_D28_S28 AND a.direction<>b.direction   "+
			" WHERE ( power(a.coord_x-b.coord_x,2)+ power(a.coord_y-b.coord_y,2)<=10000 AND a.position_boucle<=b.position_boucle) OR ( power(a.coord_x-b.coord_x,2)+power(a.coord_y-b.coord_y,2)>10000 AND a.proximite_arret<=b.proximite_arret) OR b.no_arret_deb IS NULL) as z  "+
			" ON x.ID_D28_S28=z.ID_D28_S28   "+
			" WHERE x.imput_deb is null ";
			
		UPDATE_serveur_debarq = "UPDATE a" + 
			" SET a.no_arret_deb=c.no_arret_deb , a.imput_deb= " + global.Code_imputation_debarquement + 
			" FROM dbo.CAP_GFI_" + global.ANNEEopus4 + " as a INNER JOIN arrets_probables as b ON a.ID_D28_s28= b.ID_D28_s28  " +
			" INNER JOIN arrets_possibles as c ON b.ID_D28_S28=c.ID_D28_S28 AND b.proximite_arret=c.proximite_arret  " +
			" WHERE a.imput_deb is null; ";


		let reqq=[UPDATE_CAP_GFI_debarq,UPDATE_serveur_debarq],
		reqqno=["UPDATE_CAP_GFI_debarq","UPDATE_serveur_debarq"], i=0;
		function worker(i){
			new sql.Request().query(reqq[i], function (err, getdata) {
				if (err){ console.log(reqqno[i]); throw err; }
				else { 
					confirmation(i, reqqno, getdata);
					i++;
					if(i>=reqq.length){ 
						 console.log(ftimestamp(new Date())+' - UPDATE_CAP_GFI_debarq exécutée avec succès')
						fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
							" Attribution d'arrêts de débarquement Validations Bus -- Métro  - avec succès - "+ftimestamp(new Date())+'\n'
							,function (err) {if (err) throw err;}
						);
						dmetro300()
					}else worker(i);	
				}
			});
		}worker(i);
	} 
	
	//then nettoyage de tables temporaires 16,17 et 18
	function dmetro300(){
		let rsql=[Vidange_table_temporaire_16+' '+Vidange_table_temporaire_17+' '+Vidange_table_temporaire_18];
		new sql.Request().query(rsql, function (err, getdata) {
			if (err) console.log(err); 
			else { 
				console.log('\n'+ftimestamp(new Date())+" - Vidange des tables temporaires Vidange_table_temporaire_16, Vidange_table_temporaire_17, Vidange_table_temporaire_18 avec succès");
				fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
					ftimestamp(new Date())+ " - Vidange des tables temporaires Vidange_table_temporaire_16, Vidange_table_temporaire_17, Vidange_table_temporaire_18 avec succès"
					,function (err) {if (err) throw err;}
				);
				debarquements_bus_bus();
			}	
		});
	} dmetro100();
}

//'Attribution des débarquements dans l'agglo
async function debarquements_bus_bus(){
	function dbus100(){ 
		let reqq=[ALIMENTATION_validations_successives_bus, Debarquements_possibles_RTL_hre_reel_pre, Determination_debarquement_probable],
		reqqno=["ALIMENTATION_validations_successives_bus", "Debarquements_possibles_RTL_hre_reel_pre", "Determination_debarquement_probable"], i=0;
		function worker(i){
			new sql.Request().query(reqq[i], function (err, getdata) {
				if (err){ 
					console.log(reqqno[i]); throw err; 
				}
				else { 
					confirmation(i, reqqno, getdata);
					i++;
					if(i>=reqq.length){ 
						 console.log(ftimestamp(new Date())+' - Attribution des débarquements dans l\'agglo complétée avec succès ');
						 global.Code_imputation_debarquement = 2; dbus200();
					}else worker(i);	
				}
			});
		}worker(i);
	}

//then update_CAP_GFI_debarq ­­­code imputation =2
	function dbus200(){
		let UPDATE_CAP_GFI_debarq = " UPDATE CAP_GFI_temp "+
			" SET CAP_GFI_temp.no_arret_deb=z.no_arret_deb , CAP_GFI_temp.imput_deb=  " + global.Code_imputation_debarquement +
			" FROM CAP_GFI_temp as x INNER JOIN ( "+
			" 	SELECT a.ID_D28_S28 ,a.no_arret_deb FROM ( "+
			"		SELECT aa.ID_D28_s28 ,bb.no_arret_deb ,aa.proximite_arret , "+
			" 		bb.position_boucle ,bb.direction ,bb.coord_x ,bb.coord_y FROM arrets_probables as aa  "+
			" 		INNER JOIN arrets_possibles as bb "+
			" 		ON aa.ID_D28_s28= bb.ID_D28_s28 AND aa.direction=bb.direction AND aa.proximite_arret=bb.proximite_arret"+
			"	)as a  "+
			" 	LEFT JOIN ( "+
			"		SELECT cc.ID_D28_s28 ,dd.no_arret_deb ,cc.proximite_arret ,dd.position_boucle ,dd.direction ,dd.coord_x ,dd.coord_y "+
			"		FROM arrets_probables as cc   "+
			" 		INNER JOIN arrets_possibles as dd   "+
			" 		ON cc.ID_D28_s28= dd.ID_D28_s28 AND cc.direction=dd.direction AND cc.proximite_arret=dd.proximite_arret"+
			"	) as b "+
			" 	ON a.ID_D28_S28=b.ID_D28_S28 AND a.direction<>b.direction   "+
			" 	WHERE ( power(a.coord_x-b.coord_x,2)+ power(a.coord_y-b.coord_y,2)<=10000 AND a.position_boucle<=b.position_boucle) "+
			"	OR ( power(a.coord_x-b.coord_x,2)+power(a.coord_y-b.coord_y,2)>10000 AND a.proximite_arret<=b.proximite_arret) "+
			"	OR b.no_arret_deb IS NULL "+
			" ) as z  ON x.ID_D28_S28=z.ID_D28_S28 WHERE x.imput_deb is null ";
			
		let UPDATE_serveur_debarq = "UPDATE a" + 
			" SET a.no_arret_deb=c.no_arret_deb , a.imput_deb= " + global.Code_imputation_debarquement + 
			" FROM dbo.CAP_GFI_" + global.ANNEEopus4 + " as a INNER JOIN arrets_probables as b ON a.ID_D28_s28= b.ID_D28_s28  " +
			" INNER JOIN arrets_possibles as c ON b.ID_D28_S28=c.ID_D28_S28 AND b.proximite_arret=c.proximite_arret  " +
			" WHERE a.imput_deb is null; ";

		let reqq=[UPDATE_CAP_GFI_debarq,UPDATE_serveur_debarq],
		reqqno=["UPDATE_CAP_GFI_debarq","UPDATE_serveur_debarq"], i=0;
		function worker(i){
			new sql.Request().query(reqq[i], function (err, getdata) {
				if (err){ console.log(reqqno[i]);throw err; }
				else { 
					confirmation(i, reqqno, getdata);
					i++;
					if(i>=reqq.length){ 
						console.log(ftimestamp(new Date())+' - UPDATE_CAP_GFI_debarq exécutée avec succès')
						fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
							" Attribution d'arrêts de débarquement Validations Bus -- Bus  - avec succès - "+ftimestamp(new Date())+'\n'
							,function (err) {if (err) throw err;}
						);
						dbus300()
					}else worker(i);	
				}
			});
		}worker(i);	
	} 
	
//then nettoyage de tables temporaires 16,17 et 18
	function dbus300(){
		let rsql=[Vidange_table_temporaire_16+' '+Vidange_table_temporaire_17 +' '+Vidange_table_temporaire_18];
		new sql.Request().query(rsql, function (err, getdata) {
			if (err) console.log(err); 
			else { 
				console.log(ftimestamp(new Date())+' - Vidange des tables complétée avec succès ');
				fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
					"Vidange des tables temporaires : "+ rsql + "- avec succès -"+ftimestamp(new Date())+'\n'
					,function (err) {if (err) throw err;}
				);
				debarquements_retour_domicile();
			}	
		});
	}
	dbus100();
}

//'Attribution des débarquements dernier voyage de la journée (retour au domicile) 
async function debarquements_retour_domicile(){	
	
	function ddomi100(){ 
		let reqq=[ALIMENTATION_table_dernieres_validations,  ALIMENTATION_validations_successives_retour_dom, Debarquements_possibles_Retour_Domicile, Determination_debarquement_probable],
		reqqno=["ALIMENTATION_table_dernieres_validations", "ALIMENTATION_validations_successives_retour_dom", "Debarquements_possibles_Retour_Domicile", "Determination_debarquement_probable"],i=0;
		function worker(i){
			new sql.Request().query(reqq[i], function (err, getdata) {
				if (err){ console.log(reqqno[i]);  throw err; }
				else { 
					confirmation(i, reqqno, getdata);
					i++;
					if(i>=reqq.length){ 
						 console.log(ftimestamp(new Date())+' - Attribution des débarquements de retour à domicile complétée avec succès ');
						 global.Code_imputation_debarquement = 3; ddomi200()
					}else worker(i);	
				}
			});
		}worker(i);	
	}

//then update_CAP_GFI_debarq ­­­code imputation =3
	function ddomi200(){
		let UPDATE_CAP_GFI_debarq = " UPDATE CAP_GFI_temp "+
			" SET CAP_GFI_temp.no_arret_deb=z.no_arret_deb , CAP_GFI_temp.imput_deb=  " + global.Code_imputation_debarquement +
			" FROM CAP_GFI_temp as x INNER JOIN (SELECT a.ID_D28_S28 ,a.no_arret_deb FROM (SELECT aa.ID_D28_s28 ,bb.no_arret_deb ,aa.proximite_arret , "+
			" bb.position_boucle ,bb.direction ,bb.coord_x ,bb.coord_y FROM arrets_probables as aa  "+
			" INNER JOIN arrets_possibles as bb "+
			" ON aa.ID_D28_s28= bb.ID_D28_s28 AND aa.direction=bb.direction AND aa.proximite_arret=bb.proximite_arret) as a  "+
			" LEFT JOIN (SELECT cc.ID_D28_s28 ,dd.no_arret_deb ,cc.proximite_arret ,dd.position_boucle ,dd.direction ,dd.coord_x ,dd.coord_y FROM arrets_probables as cc   "+
			" INNER JOIN arrets_possibles as dd   "+
			" ON cc.ID_D28_s28= dd.ID_D28_s28 AND cc.direction=dd.direction AND cc.proximite_arret=dd.proximite_arret) as b   "+
			" ON a.ID_D28_S28=b.ID_D28_S28 AND a.direction<>b.direction   "+
			" WHERE ( power(a.coord_x-b.coord_x,2)+ power(a.coord_y-b.coord_y,2)<=10000 AND a.position_boucle<=b.position_boucle)"+
			" OR ( power(a.coord_x-b.coord_x,2)+power(a.coord_y-b.coord_y,2)>10000 AND a.proximite_arret<=b.proximite_arret) OR b.no_arret_deb IS NULL) as z  "+
			" ON x.ID_D28_S28=z.ID_D28_S28  "+
			" WHERE x.imput_deb is null ";
			
		let UPDATE_serveur_debarq = "UPDATE a "+
			" SET a.no_arret_deb=c.no_arret_deb , a.imput_deb= " + global.Code_imputation_debarquement + 
			" FROM dbo.CAP_GFI_" + global.ANNEEopus4 + " as a INNER JOIN arrets_probables as b ON a.ID_D28_s28= b.ID_D28_s28  " +
			" INNER JOIN arrets_possibles as c ON b.ID_D28_S28=c.ID_D28_S28 AND b.proximite_arret=c.proximite_arret  " +
			" WHERE a.imput_deb is null; ";
	
		let reqq=[UPDATE_CAP_GFI_debarq, UPDATE_serveur_debarq],
		reqqno=["UPDATE_CAP_GFI_debarq", "UPDATE_serveur_debarq"], i=0;
		function worker(i){
			new sql.Request().query(reqq[i], function (err, getdata) {
				if (err){ console.log(reqqno[i]);throw err; }
				else { 
					confirmation(i, reqqno, getdata);
					i++;
					if(i>=reqq.length){ 
						console.log(ftimestamp(new Date())+' - UPDATE_CAP_GFI_debarq exécutée avec succès')
						fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
							'\n'+ftimestamp(new Date())+"Attribution d'arrêts de débarquement Retour au domicile - avec succès - "
							,function (err) {
								if (err) throw err;
							}
						);
						ddomi300();
					}else worker(i);	
				}
			});
		}worker(i);	
	}

//then nettoyage de tables temporaires 16,17 et 18
	function ddomi300(){
		let rsql=[Vidange_table_temporaire_16+' '+Vidange_table_temporaire_17+' '+Vidange_table_temporaire_18+' '+Vidange_table_temporaire_19];
		new sql.Request().query(rsql, function (err, getdata) {
			if (err) console.log(err); 
			else { 
				console.log(ftimestamp(new Date()) +" - Vidange des tables temporaires 16, 17, 18 et 19 exécutée avec succès\n");
				fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
					"Vidange des tables temporaires :  16, 17, 18 et 19 avec succès -"+ftimestamp(new Date())+'\n'
					,function (err) {if (err) throw err;}
				); correction();
			}
		}); 
	} ddomi100();
}	

//'Correction des arrêts de débarquement identiques ou trop proche (- de 300m) aux arrêts d'embarquement	
function correction(){ 
	let reqq=[Correction_emb_egal_deb, Correction_emb_proche_deb], reqqno=["Correction_emb_egal_deb", "Correction_emb_proche_deb"], i=0;
		function worker(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ console.log(reqqno[i]); throw err; }
			else { 
				confirmation(i, reqqno, getdata);
				i++;
				if(i>=reqq.length){ 
					 console.log(ftimestamp(new Date())+' - 1ere Correction des arrêts de débarquement identiques ou trop proche (- de 300m) aux arrêts d\'embarquement complétée avec succès ');
					 historique_arret_deb();
				}else worker(i);	
			}
		});
	}worker(i);
}

// 'Attribution des débarquements selon habitude de l'utilisateur  //Cette requête depend de la table dbo.CAP_GFI_2018 //+ChoixAnnee
async function historique_arret_deb(){
	let anneeopus4b='', vv=global.ANNEEopus4;
	//si mois est janvier considérer les historiques de la BDD de l'annéee précédente
	let mm=new Date((+global.DernierJourTraite) + 86400000*1).getMonth();
 	if(mm==0) vv='1201';
	
	let Determination_historique_deb =  " INSERT INTO arrets_possibles "+
		" SELECT a.ID_D28_S28 ,b.identification , null as [no_arret] , null as [no_arret_emb] "+
		" ,b.[no_arret_deb] , null as [direction] , null as [position] , null as [position_boucle] "+
		" ,null as [position_arret_emb] ,null as [coord_x] ,null as [coord_x_emb] ,null as [coord_y]"+
		" ,null as [coord_y_emb] ,COUNT(*) as [nombre_occ] ,null as [proximite_temporelle] ,null as [proximite_arret]"+
		" FROM (CAP_GFI_temp AS a INNER JOIN dbo.CAP_GFI_"+vv+" AS b ON a.identification=b.identification "+
		" AND CAST(a.date28 AS DATETIME)-21 <= b.date28 AND a.date28 > b.date28 AND a.no_arret_emb=b.no_arret_emb "+
		" AND a.seconde28-600 <= b.seconde28 AND a.seconde28+600 >= b.seconde28 AND (a.ligne=b.ligne or a.lig_cap=b.ligne or a.ligne=b.lig_cap) AND a.direction=b.direction) "+
		" INNER JOIN extract_ref_course_arret as c ON a.voiture=c.voiture AND a.hre_pre_dep=c.hre_pre_dep AND b.no_arret_emb=c.no_arret "+
		" WHERE b.imput_deb<>0 AND a.imput_deb=0 OR (b.imput_deb IS NOT NULL AND a.imput_deb IS NULL) "+ 
		" GROUP BY a.ID_D28_S28 ,b.identification ,b.no_arret_deb;";
	
	let UPDATE_serveur_debarq = "UPDATE a" +
		" SET a.no_arret_deb=c.no_arret_deb , a.imput_deb= " + global.Code_imputation_debarquement + 
		" FROM dbo.CAP_GFI_" + global.ANNEEopus4 + " as a INNER JOIN arrets_probables as b ON a.ID_D28_s28= b.ID_D28_s28  " +
		" INNER JOIN arrets_possibles as c ON b.ID_D28_S28=c.ID_D28_S28 AND b.proximite_arret=c.proximite_arret  " +
		" WHERE a.imput_deb is null; ";

	function histdeb100(){
		console.log('Fonction histdeb100() est appelée')
		new sql.Request().query(Determination_historique_deb, function(err, getdata) {
			if (err) console.log(err); 
			else { 
				console.log("Attribution d'arrêts de debarquement HISTORIQUE ("+getdata.rowsAffected[0]+ ' lignes)');
				fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
					"\n Attribution d'arrêts de debarquement HISTORIQUE -"+ftimestamp(new Date())+'\n'
					,function (err) {if (err) throw err;}
				); histdeb200();
			}
		});	 
	}histdeb100();	
	
	function histdeb200() {
		console.log('Fonction histdeb200() est appelée')
		let reqq=[Determination_meilleur_historique, UPDATE_CAP_GFI_debarq_histo, UPDATE_serveur_debarq, Correction_emb_egal_deb, Correction_emb_proche_deb], 
		reqqno=["Determination_meilleur_historique", "UPDATE_CAP_GFI_debarq_histo", "UPDATE_serveur_debarq", "Correction_emb_egal_deb", "Correction_emb_proche_deb"],i=0;
		function worker(i){
			new sql.Request().query(reqq[i], function (err, getdata) {
				if (err){ console.log(reqqno[i]); throw err; }
				else { 
					confirmation(i, reqqno, getdata);
					if (i==3) console.log(ftimestamp(new Date())+" - 2e Correction des arrêts de débarquement identiques ou trop proche (- de 300m) aux arrêts d\'embarquement - avec succès("+getdata.rowsAffected[0]+ ' lignes)');
					i++;
					if(i>=reqq.length){ 
						console.log(ftimestamp(new Date())+" - Attribution d'arrêts de debarquement HISTORIQUE -- complétée avec succès ("+getdata.rowsAffected[0]+' lignes)');
						fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
							" \nCorrection des arrêts de débarquement identiques ou trop proche (- de 300m) aux arrêts d'embarquement - avec succès"+ftimestamp(new Date())+'\n'
							,function (err) {if (err) throw err;}
						);
						console.log('Processus complété avec succès - Voir table cap_gfi_'+global.ANNEEopus4);
						gencat();
					}else worker(i);	
				}
			});
		}worker(i);
	}
}	

// Générer les catégories de titre
function gencat(){
	console.log(ftimestamp(new Date())+ ' - Fonction gencat() est appelée pour générer les catégories de titre');
	
	// Ajouter les catégories de titres
	let categorie= "  alter table [AchalDep].[dbo].[CAP_GFI_temp] add categorie varchar(100); ",
		gratuit=" Update CAP_GFI_temp Set categorie='Gratuit' where code_titre IN ('88','168', '161', '162','163','164','165','166','167','168','169','170','172','175','200','201','202','EMPRC','titre bidon'); ",
		scolaire = " Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Scolaires' where code_titre IN ('SCOL6','6MOIS') ;",
		annee65 = " Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Ainés - 65 ans' where code_titre IN  ('Bouch', 'Bross', 'StLam', 'AccÃ¨s', 'HRSPO', 'AinÃ©', 'Ainé', 'Accès') ;",
		billets = " Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Billets' where code_titre IN  ('12BOJ', '18BOJ', '1BOCJ', '24BOJ', '2BOCJ', '6BO', '6BOCJ', '6BR'); ",
		passagess= " Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Passages' where code_titre IN ('1pacj', '1pass', '2pacj', '3pacj', '4pacj') ;",
		tram3_sans = " Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Tram 3 - sans abonnement' where code_titre IN ('3', '29','41', '133', '143', '145', '153'); ",
		tram3_avec = " Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Tram 3 - avec abonnement' where code_titre IN  ('81', '91', '99') ",
		tram4_8_sans = " Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Tram 4 à 8 - sans abonnement' where code_titre IN  ('4', '5', '6', '7', '8', '30', '30', '31', '32', '33', '34', '42', '43', '44', '45', '46', '135', '156'); ",
		tram4_8_avec = " Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Tram 4 à 8 - avec abonnement' where code_titre IN ('82', '83', '84', '85', '86', '92', '93', '94', '95', '96', '100', '101', '102', '103', '104'); ",
		clo_sans = "  Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Cartes locales ordinaires - sans abonnement' where code_titre IN ('CLO', 'POSTE'); ",
		clo_avec = "  Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Cartes locales ordinaires - avec abonnement' where code_titre = 'TRCLO'; ",
		clr_sans = "  Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Cartes locales reduites - sans abonnement' where code_titre IN('CLR',  '4MRÉD', '4MRED'); ",
		clr_avec = "  Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Cartes locales reduites - avec abonnement' where code_titre = 'TRCLR' ",
		journalier = " Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Journaliers' where code_titre IN ('1jour', '1JOUR', '1soir', '1SOIR'); ",
		comptant = "   Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Comptants' where code_titre='COMPT' ;",
		autre ="   Update [AchalDep].[dbo].[CAP_GFI_temp]  Set categorie='Autres' where categorie is null ;";
	
	//Corriger les cas où la colonne période est vide alors que l'heure (seconde28) est bien non vide	
	let periode = " use achaldep; update cap_gfi_temp set periode= IIF(seconde28 between 14400 and 21599, 'MA', "+
				  " iif( seconde28 between 21600 and 32399, 'AM', iif( seconde28 between 32400 and 55799, 'HP', "+
				  " iif(seconde28 between 55800 and 66599, 'PM', iif(seconde28 >66599, 'SO',''))))) from cap_gfi_temp;	";
	
	let reqq =[periode, categorie, gratuit, scolaire, annee65, billets, passagess, tram3_sans, tram3_avec, tram4_8_sans, tram4_8_avec, clo_sans, clo_avec, clr_sans, clr_avec, journalier, comptant, autre];
	let reqqno =["periode", "categorie", " gratuit", " scolaire", " annee65", " billets", " passagess", " tram3_sans", " tram3_avec", " tram4_8_sans", " tram4_8_avec", " clo_sans", " clo_avec", " clr_sans", " clr_avec", " journalier", " comptant", "autre"];
	let i=0;
	
	function worker(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ 
				console.log(reqqno[i]);  
				console.log(reqq[i]);
				throw err; 
			}
			else { 
				confirmation(i, reqqno, getdata);
				i++;
				if(i>reqq.length){ 
					transfert_bd_Opus();
					console.log(ftimestamp(new Date())+ ' - Les catégories de titre sont générées avec succès ('+getdata.rowsAffected[0]+' lignes )');
					fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
						"\n"+ftimestamp(new Date())+" -- Les catégories de titre sont générées avec succès ("+getdata.rowsAffected[0]+' lignes )'
						,function (err) {
							if (err) throw err;
						}
					)
				}
				else worker(i);	
			}
		});
	}worker(i);	
}

//***********FIN DES MANIPULATIONS ****************

//'transfert vers la BD temporaire sur le serveur (sert à accélerer le transfert) 
function transfert_bd_Opus(){
	Transfert_BD_OPUS = " INSERT INTO [AchalDep].[dbo].CAP_GFI_"+global.ANNEEopus4+ " SELECT * FROM [AchalDep].[dbo].[CAP_GFI_temp];";
	
	//Mettre à jour la table cap_gfi_1201 avec les prédictions de janvier 
	let Cap_gfi_dec21 = " insert into cap_gfi_1201 Select * from [AchalDep].[dbo].[CAP_GFI_temp];";
	
	let mm=new Date((+global.DernierJourTraite) + 86400000*1).getMonth(), 
	aa=new Date((+global.DernierJourTraite) + 86400000*1).getFullYear(), 
	jj=new Date((+global.DernierJourTraite) + 86400000*1).getDate() ;
	
	let reqq =[Transfert_BD_OPUS];  
	let reqqno =["Transfert BD_OPUS_vers_CAP_GFI_AAAA"];   
	let i=0;  
	
	if(mm==0){
		reqq =[Cap_gfi_dec21, Transfert_BD_OPUS];   
		reqqno =["Cap_gfi_dec21", "Transfert BD_OPUS_vers_CAP_GFI_AAAA"]; 
	}
	if(mm!=0) {
		reqq=["use achaldep; IF OBJECT_ID('cap_gfi_1201', 'U') IS NOT NULL DROP TABLE cap_gfi_1201;", Transfert_BD_OPUS]; //supprimer table cap_gfi_1201 au 1 février
		reqqno =["cap_gfi_1201 supprimée", "Transfert BD_OPUS_vers_CAP_GFI_AAAA"]; 
	}
	
	function worker(i){
		new sql.Request().query(reqq[i], function (err, getdata) {
			if (err){ 
				console.log(reqqno[i]);  
				console.log(reqq[i]);
				throw err; 
			}
			else { 
				confirmation(i, reqqno, getdata); 
				i++;
				if(i>=reqq.length){
					console.log(ftimestamp(new Date())+ ' - Transfert vers BD Achalandage > CAP_GFI_'+global.ANNEEopus4+
						' complété avec succès ('+getdata.rowsAffected[0]+' lignes insérées)');
					//statistiques_toutes();
					fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
						"\n"+ftimestamp(new Date())+" - Transfert vers BD CAP_GFI >>> AchalDep - avec succès "
						,function (err) {
							if (err) throw err;
						}
					)
					let tabDel =[ "CAP_GFI_temp", "t_sdap_course", "t_sdap_course_arret", "calendrier", "bus_course", 
								"boucle_course", "BD_Valid_Metro", "arrets_probables", "arrets_possibles", "arret_metro", 
								"extract_gfi","textract_gfi02", "extract_dbo_opus", "dico_titre", "dernieres_validations", 
								"chaine_validation", "chaine_validation2", "chaine_validation3", "extract_ref_arret", 
								"extract_ref_course", "extract_ref_course_arret", "extract_ref_course_arret_suivant", 
								"extract_sar_bus", "extract_sar_bus_","extract_sdap_course", "extract_sdap_course_arret", 
								"liste_arret_boucle", "ordonnancement_Valid",  "ordonnancement_Valid_2", 
								"pos_course_dans_voiture", "semaine_relache", "extract_sdap",  "chainetemp", "chainetemp1", "validations_successives" ];
							delTable(tabDel);
					setTimeout ( ()=>lastDate(), 1000*10); 
				}
				else worker(i);	
			}				
		});
	}worker(i);	
}

  // ********************************************//
 //     		THE BIG PUSH FUNCTION 			//
//*********************************************//
async function COMPILATION_OPUS_GFI(){
	console.log(  "\n4- Heure de début: "+LeDebut)
	
	console.log(ftimestamp(new Date()), ': Appel du jobber Compilation_Opus_GIF  -- OK' );
	await function (){
		//TERMINER LE PROCESSUS SI DELAI MOINS DE 15 JOURS 
		if(global.delai>=15){
			console.log(ftimestamp(new Date()), 'Étape 02 - Délai vérifié - déclenchement du processus autorisé --'+ 15-global.delai+ ' jour(s)\n');
			return  new Promise( function(resolve, reject){
				setTimeout ( function(){
					try{
						resolve(pro3());
					}catch(err){ 
						console.log(err);
					}
				},1000)
			});
		}
		
		else{
			console.log(ftimestamp(new Date()), ' Interruption du processus -- Réessayer encore dans '+ 15-global.delai+ ' jour(s)\n');
			//Rapporter l'arrêt du processus dans le log du jour
			fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
				ftimestamp(new Date())+' - Interruption du processus \n\t\t Date du dernier traitement : '+formatdate(global.dernieredate)+'\n\t\t Jours d\'attente restants:'+(15-global.delai), 
				function (err) {
					if (err) throw err;
				}
			);
		}
	}();
	
	
	async function pro3(){
		//=====================================================================================//
		//							INITITATION DU PROCESSUS 		     					  //
		//===================================================================================//

		//*******************************************//
		//            Appel des fonctions           //
		//*****************************************//	
		var f01= async function(){
			return ftimestamp(new Date());
		};
	
		//Ajouter au log le début du lancement des requetes//
		function logg(){
			console.log('Promise 6.1 --'+ ftimestamp(new Date())+' Création du log');
			//Enregistrement dans le log
			fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
				'\n'+ftimestamp(new Date())+': Lancement des requêtes SQL vers le Server MS SQL \n'
				,function (err) {
					if (err) throw err;
				}
			);
		}
		await logg();
		
		//Suppression des tables temporaires 
		new Promise( function(resolve, reject){
			setTimeout(() => resolve('GO'), 1000);
		}).then( function (){
			//SUPPRIMMER LES TABLES
			let tab2del01 = [ "CAP_GFI_temp", "t_sdap_course", "t_sdap_course_arret", "calendrier", "bus_course", 
			"boucle_course", "BD_Valid_Metro", "arrets_probables", "arrets_possibles", "arret_metro", "extract_sar_bus", 
			"extract_sar_bus_", "extract_gfi", "extract_sdap", "textract_gfi02", "extract_dbo_opus", "dico_titre", 
			"dernieres_validations", "chaine_validation", "chaine_validation2", "chaine_validation3", "extract_ref_arret", 
			"extract_ref_course", "extract_ref_course_arret", "extract_ref_course_arret_suivant", "extract_sar_bus", 
			"extract_sar_bus_","extract_sdap_course", "extract_sdap_course_arret", "liste_arret_boucle", "ordonnancement_Valid",  
			"ordonnancement_Valid_2", "pos_course_dans_voiture",  "chainetemp", "chainetemp1","semaine_relache", "validations_successives" ];
			deleteTable(tab2del01);
			return new Promise ( function(resolve, reject){
				setTimeout(() => resolve(), 15000);
			});
		})
	}	
}

let ttDebut= ftimestamp(new Date());
global.dd=new Date();
const LeDebut=ftimestamp(new Date());
	
//Déclenchement du Processus sollicité
console.log('\n'+ ttDebut+": Tentative de lancement du Processus d'intégration de données vers Achalandage\n");

///'Détermination du dernier jour déjà traité	
function lastDate(){
	const jourdhui= new Date(), c_an= jourdhui.getFullYear(), c_mois= jourdhui.getMonth()+1, c_jour=jourdhui.getDate();
	var ChoixAnnee = c_an;

	//decommenter pour traiter une annee autre que celle en cours
	//ChoixAnnee=2017;
	
	//Decommenter la ligne suivante si mode automatisé
	//if(c_mois==1 && c_jour<15) ChoixAnnee=c_an-1; //si on est pas encore 15 janvier, considérer annee précédente
	
	var annee2 = ChoixAnnee-2000; //parametrable	
	sql.close(); 
	sql.connect(config, function (err){
		if (err) {
			fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt",  
				'\n'+ttDebut+": Tentative de lancement du Processus d'intégration de données \n"+
				ftimestamp(new Date())+' - Date de création de ce log \n'+
				ftimestamp(new Date())+' - Connection au Serveur MS SQL Planif_Works -- Échouée\n',
				function (err) {
					if (err) throw err;
				}
			);
			console.log(ftimestamp(new Date())+": Étape 01 - connection au serveur SQL Planif_Works échouée \n"+ err);
		}
		else{
			console.log(
				'\n \n||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\n'+
				'||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\n'+
				ftimestamp(new Date())+": Demande de connection au serveur SQL Planif_Works réussie \n"
			);
			
			setTimeout( function(){
				fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
					'\n \n||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\n'+
					'||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\n'+
					ttDebut+" - Tentative de lancement du Processus d'intégration de données \n"+
					ftimestamp(new Date())+' - Date de création de ce fichier log \n'+
					ftimestamp(new Date())+' - Connection au Serveur MS SQL Planif_Works -- OK\n'+
					ftimestamp(new Date())+' - Demande de la dernière date de traitement au Serveur Planif_Works -- en attente',
					function (err) {
						if (err) throw err;
					}
				)
			}, 5000)
			
			//Requete Date du dernier traitement  -- SQL Query
			let request = new sql.Request();
			var lastdate, lastdateOP; let sdate=0;	//indiquer ici la date à traiter en modifiant la valeur de sdate (par 0 pour garder la dernière date de mise a jour, un nombre x de jour à reculer)
			
			//si la table n'existe pas, créer la avant d'y transferer les donnéees 
			let createit= "use achaldep; IF OBJECT_ID('[AchalDep].[dbo].CAP_GFI_"+ChoixAnnee+"', 'U') IS NULL select top (1) * into [AchalDep].[dbo].CAP_GFI_"+ChoixAnnee +
			" from cap_gfi_2019; Truncate table [AchalDep].[dbo].CAP_GFI_"+ChoixAnnee;
					
			let RQ_JourTraites = "SELECT distinct MAX(date24) as dernieredate FROM dbo.CAP_GFI_"+ChoixAnnee;
			let RQ_JourOPus = "SELECT distinct MAX(DTHR_Operation) as dernierOP FROM [Opus"+annee2+"].[dbo].[Opus];" //opus18.dbo.opus
			let i=1;
			let reqq =[ createit, RQ_JourTraites, RQ_JourOPus ], reqqno =["createit", "RQ_JourTraites", "RQ_JourOPus"];
					
			function worker(i){
				new sql.Request().query(reqq[i], function (err, getdata) {
					if (err){ 
						i=0;
						worker(i);
						console.log(reqq[i+1]+'\n' + err); 
						
					}
					else { 
						
						if(i==1) {
							confirmation(i, reqqno, getdata);
							global.lastdate = (getdata.recordset[0].dernieredate);
							console.log('Dernier Jour traité (lastdate):', global.lastdate); 
							if(global.lastdate == null) global.lastdate=new Date(ChoixAnnee+'-01-01'); // si pas de données, debut == jour nouvel an
							
							//decommenter pour traiter une date spécifique
							//global.lastdate = new Date ('2018-01-01'); 
							global.DernierJourTraite= new Date((global.lastdate)-86400000*sdate);  
							console.log('\nDernier jour traité: ', global.DernierJourTraite);
							fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
								'\n\n||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||\n\n'+
								ftimestamp(new Date())+' - Dernière date traitée : '+formatdate01(global.lastdate)
								,function (err) {
									if (err) throw err;
								}
							)
							i++;							
						}
						else if (i==2){
							global.lastdateOP = new Date(getdata.recordset[0].dernierOP);
							//indiquer ici la date à traiter en modifiant la valeur de 
							//sdate (par 0 pour garder la dernière date de mise a jour, un nombre x de jour à reculer)  
							console.log('Dernier jour OP: ', global.lastdateOP);	
							i++;
						}
						
						if(i>=3){ 
							global.nbjours=diffDate(global.lastdateOP, global.DernierJourTraite);
							let mm=new Date(global.lastdateOP).getMonth()+1, jj=new Date(global.lastdateOP).getDate() ;
							console.log(ftimestamp(new Date())+' - il y a '+ global.nbjours+' jour(s) à traiter !!!');
							//si dernier jour OPus n'est pas pas premier janvier
							if((!(mm==12 && jj==31)) && global.nbjours<15){ 
								console.log( 'Tous les jours sont déjà traités. Le programme va s\'arrêter, supprimer les tables temporaires, et sera automatiquement relancé dans 48 heures!');
								let tab2Drop =[ "CAP_GFI_temp", "calendrier", "bus_course", "boucle_course", "BD_Valid_Metro", "arrets_probables", 
								"arrets_possibles", "arret_metro", "extract_gfi","textract_gfi02", "extract_dbo_opus", "dico_titre", 
								"dernieres_validations", "chaine_validation", "chaine_validation2", "chaine_validation3", "extract_ref_arret", 
								"extract_ref_course", "extract_ref_course_arret", "extract_ref_course_arret_suivant", "extract_sar_bus", 
								"extract_sar_bus_","extract_sdap_course", "extract_sdap_course_arret", "liste_arret_boucle", "ordonnancement_Valid",  
								"ordonnancement_Valid_2", "pos_course_dans_voiture", "semaine_relache", "chainetemp", "chainetemp1", "validations_successives" ];
								delTable(tab2Drop);
								setTimeout(()=>{lastDate()}, 1000*60*60*24*2)
							}
							else promise2();
						}else worker(i);
					}
				});
			}worker(i);	
		}
	});
} ;

//'création d'un fichier log .txt == les traces du déroulement = 'création d'un fichier .txt journal du déroulement
//Création du fichier log du jour (nom <log-Date du jour (aaaa-mm-jj).log>
function promise2(){
	sql.close;
	return new Promise( function(resolve, reject){
		setTimeout( function(){		
			console.log(ftimestamp(new Date())+": Inscrire démarrage du processus au fichier log du jour\n");
			
			//Création du log du jour (nom <log-Date du jour (aaaa-mm-jj).log>
			fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt", 
				'\n------------------------------------------------------------------------\n'+
				'TRAITEMENT DES VALIDATIONS CARTE À PUCE ET PAIEMENTS À BORD DU '+
				formatdate01(global.lastdate)+'\n'+
				'------------------------------------------------------------------------\n'
				,function (err) {
					if (err) throw err;
					else resolve(checkdelay());
				}
			);
			
		}, 2000);	
	});
}
	
//Calculer nombre de jours écoulés depuis le dernier traitement
function checkdelay(){
	console.log( global.dd , '-', global.DernierJourTraite);
	let diff = diffDate(global.dd, global.DernierJourTraite);
	global.delai= diff;
	console.log("Vérification du nombre de jours écoulés depuis le dernier traitement: "+ global.delai);
	
	fs.appendFile("./data/log/log-"+formatdate(global.dd)+".txt",
		ftimestamp(new Date())+': Vérification du nombre de jours écoulés depuis le dernier traitement :'+ 15-global.delai+ ' jour(s)\n',
		function (err) {
			if (err) throw err;
			else {
				return  new Promise( function(resolve, reject){
					setTimeout ( function(){
						console.log("\n#3 - promise checkdelay complété\n");
						resolve(genDate());
					},3000)
				});
			}
		}
	)
};
	
//===========================================//
	lastDate();
	module.exports = router;
//=========================================//						

