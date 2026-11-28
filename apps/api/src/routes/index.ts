import { Router } from "express";
import { metricsRoutePrefix } from "../metrics/http.js";
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

function mount(prefix: string, router: Router) {
  routes.use(prefix, metricsRoutePrefix(prefix), router);
}

routes.get("/health", getHealth);
mount(`/api/${env.API_VERSION}/health`, healthRouter);
mount(`/api/${env.API_VERSION}/auth/github`, githubAuthRouter);
mount(`/api/${env.API_VERSION}/auth`, authRouter);
mount(`/api/${env.API_VERSION}/github`, githubAppRouter);
mount(`/api/${env.API_VERSION}/projects`, projectEnvironmentVariableRouter);
mount(`/api/${env.API_VERSION}/projects`, projectRepositoryRouter);
mount(`/api/${env.API_VERSION}/projects`, projectBuildConfigurationRouter);
mount(`/api/${env.API_VERSION}/projects`, projectDeploymentRouter);
mount(`/api/${env.API_VERSION}/projects`, deploymentRecoveryRouter);
mount(`/api/${env.API_VERSION}/projects`, projectDeploymentLogsRouter);
mount(`/api/${env.API_VERSION}`, workspaceRouter);
