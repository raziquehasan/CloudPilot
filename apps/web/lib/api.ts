/**
 * CloudPilot — Central API Route Registration
 *
 * Phase 5.6 — Realtime Deployment Logs
 *
 * Responsibilities:
 * - Centralized API version prefix.
 * - Existing route ordering preservation.
 * - Deployment log endpoint registration.
 * - Backward-compatible API routing.
 *
 * Authentication and authorization are enforced
 * by the corresponding routers and controllers.
 */

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
import { projectDeploymentLogsRouter } from "../controllers/deployment-logs.controller.js";

// ==================================================
// API prefixes
// ==================================================

const API_PREFIX = `/api/${env.API_VERSION}`;

const PROJECTS_PREFIX = `${API_PREFIX}/projects`;

// ==================================================
// Central router
// ==================================================

export const routes = Router();

// ==================================================
// Health endpoints
// ==================================================

routes.get("/health", getHealth);

routes.use(
  `${API_PREFIX}/health`,
  healthRouter,
);

// ==================================================
// Authentication endpoints
// ==================================================

routes.use(
  `${API_PREFIX}/auth/github`,
  githubAuthRouter,
);

routes.use(
  `${API_PREFIX}/auth`,
  authRouter,
);

// ==================================================
// GitHub App integration
// ==================================================

routes.use(
  `${API_PREFIX}/github`,
  githubAppRouter,
);

// ==================================================
// Project environment variables
// ==================================================

routes.use(
  PROJECTS_PREFIX,
  projectEnvironmentVariableRouter,
);

// ==================================================
// Project repository integration
// ==================================================

routes.use(
  PROJECTS_PREFIX,
  projectRepositoryRouter,
);

// ==================================================
// Project build configuration
// ==================================================

routes.use(
  PROJECTS_PREFIX,
  projectBuildConfigurationRouter,
);

// ==================================================
// Deployment lifecycle
// ==================================================

routes.use(
  PROJECTS_PREFIX,
  projectDeploymentRouter,
);

// ==================================================
// Phase 5.6 — Deployment logs
// ==================================================
//
// GET /projects/:projectId/deployments/:deploymentId/logs
//
// GET /projects/:projectId/deployments/:deploymentId/logs/download
//
// Authentication, workspace authorization, query validation
// and feature flag enforcement are handled by the controller.

routes.use(
  PROJECTS_PREFIX,
  projectDeploymentLogsRouter,
);

// ==================================================
// Workspace endpoints
// ==================================================

routes.use(
  API_PREFIX,
  workspaceRouter,
);