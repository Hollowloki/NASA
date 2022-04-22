const express = require('express');

const {
    httpGetAllLaunches, httpAddNewLaunch, httpAbortLaunch
} = require('./launches.controller');

const launchesRouter = express.Router();


launchesRouter.get('/', httpGetAllLaunches);
launchesRouter.post('/', httpAddNewLaunch);
launchesRouter.delete('/:Launchesid', httpAbortLaunch);

module.exports = launchesRouter;
