import { Router } from "express";
import { env } from "../config/env.js";
import { getHealth } from "../controllers/health.controller.js";
import { healthRouter } from "./health.routes.js";
import { githubAuthRouter } from "./github-auth.routes.js";
import { authRouter } from "../controllers/auth.controller.js";
import { workspaceRouter } from "../controllers/workspace.controller.js";
import { githubAppRouter } from "../controllers/github-app.controller.js";
import { projectRepositoryRouter } from "../controllers/project-repository.controller.js";
import { projectBuildConfigurationRouter } from "../controllers/project-build-configuration.controller.js";
import { projectEnvironmentVariableRouter } from "../controllers/project-environment-variable.controller.js";
import { projectDeploymentRouter } from "../controllers/project-deployment.controller.js";
import { deploymentRecoveryRouter } from "../controllers/deployment-recovery.controller.js";
import { projectDeploymentLogsRouter } from "../controllers/deployment-logs.controller.js";

export const routes = Router();

routes.get("/health", getHealth);
routes.use(`/api/${env.API_VERSION}/health`, healthRouter);
routes.use(`/api/${env.API_VERSION}/auth/github`, githubAuthRouter);
routes.use(`/api/${env.API_VERSION}/auth`, authRouter);
routes.use(`/api/${env.API_VERSION}/github`, githubAppRouter);
routes.use(`/api/${env.API_VERSION}/projects`, projectEnvironmentVariableRouter);
routes.use(`/api/${env.API_VERSION}/projects`, projectRepositoryRouter);
routes.use(`/api/${env.API_VERSION}/projects`, projectBuildConfigurationRouter);
routes.use(`/api/${env.API_VERSION}/projects`, projectDeploymentRouter);
routes.use(`/api/${env.API_VERSION}/projects`, deploymentRecoveryRouter);
routes.use(`/api/${env.API_VERSION}/projects`, projectDeploymentLogsRouter);
routes.use(`/api/${env.API_VERSION}`, workspaceRouter);
