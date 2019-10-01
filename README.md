# algo_api
Programme NodeJS implémentant un algorithme d'intégration et d'enrichissement des données de validation par cartes à puce à bord des autobus et celles des paiements en comptant provenant des boites de perception avec les données d'opérationnalisation du réseau dont les  les assignations et les horaires, les données relatives aux routes et arrêts, etc. Après la phase d'intégration, le programme implémente un second algorithme permettant d'attribuer un arrêt de montée et un arrêt de descente, deux éléments, absents dans les données initiales, pourtant essentiels pour l'analyse et le suivi de l'achalandage du réseau de transport.

Le Programme spécifie et lance un serveur NodeJS (app18). Ce dernier se charge selon les différentes étapes et directives (app20180208.js) de l'interaction avec les différentes bases de données (STAD, OPUS, GFI).

Sans interface web, le programme est automate à 100%, c-à-d il sait quand s'arrêter, quand recontinuer à travailler. 

Il suffit de  s'assurer que le serveur est en marche.
