import { Router } from "express";
import type { Request, Response } from "express";
import { z } from "zod";
import { prisma } from "@cloudpilot/database";
import { requireAuth, requireCsrf } from "../middlewares/auth.middleware.js";

const nameSchema = z.object({ name: z.string().trim().min(1).max(80) }).strict();
const projectSchema = z.object({ name: z.string().trim().min(1).max(80), description: z.string().trim().max(500).optional().nullable() }).strict();
const slugify = (value: string) => value.toLowerCase().normalize("NFKD").replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "").slice(0, 48) || "workspace";
const projectDetailsInclude = {
  repository: { select: {
    id: true, provider: true, externalId: true, name: true, fullName: true, defaultBranch: true,
    selectedBranch: true, language: true, isPrivate: true, createdAt: true, updatedAt: true,
    installation: { select: { accountLogin: true, accountType: true, status: true, repositorySelection: true } },
  } },
  deployments: { orderBy: { createdAt: "desc" as const }, take: 8, select: { id: true, status: true, environment: true, triggerSource: true, requestedBranch: true, commitSha: true, createdAt: true, startedAt: true, finishedAt: true, failureCode: true } },
  environmentVariables: { orderBy: [{ environment: "asc" as const }, { key: "asc" as const }], select: { id: true, key: true, environment: true, createdAt: true, updatedAt: true } },
  _count: { select: { deployments: true, environmentVariables: true } },
};

async function activeWorkspace(req: Request) {
  const session = await prisma.session.findUnique({ where: { id: req.sessionId! }, select: { activeWorkspaceId: true } });
  let workspaceId = session?.activeWorkspaceId;
  let membership = workspaceId ? await prisma.workspaceMember.findUnique({ where: { workspaceId_userId: { workspaceId, userId: req.userId! } }, include: { workspace: true } }) : null;
  if (!membership) {
    membership = await prisma.workspaceMember.findFirst({ where: { userId: req.userId! }, orderBy: { createdAt: "asc" }, include: { workspace: true } });
    if (membership && session) await prisma.session.update({ where: { id: req.sessionId! }, data: { activeWorkspaceId: membership.workspaceId } });
  }
  return membership;
}
const canManage = (role: string) => role === "OWNER" || role === "ADMIN";

async function listWorkspaces(req: Request, res: Response) {
  const memberships = await prisma.workspaceMember.findMany({ where: { userId: req.userId! }, orderBy: { createdAt: "asc" } });
  const active = await activeWorkspace(req);
  const workspaceRows = await prisma.workspace.findMany({ where: { id: { in: memberships.map((membership) => membership.workspaceId) } } });
  const roleByWorkspace = new Map(memberships.map(({ workspaceId, role }) => [workspaceId, role]));
  res.json({ success: true, data: { activeWorkspaceId: active?.workspaceId ?? null, workspaces: workspaceRows.map((workspace) => ({ ...workspace, role: roleByWorkspace.get(workspace.id) })) } });
}
async function createWorkspace(req: Request, res: Response) {
  const parsed = nameSchema.safeParse(req.body);
  if (!parsed.success) { res.status(400).json({ success: false, error: { code: "VALIDATION_ERROR", message: "Enter a valid workspace name." } }); return; }
  const base = `${slugify(parsed.data.name)}-${req.userId!.slice(0, 8)}`;
  const created = await prisma.$transaction(async (tx) => {
    let slug = base; let suffix = 2;
    while (await tx.workspace.findUnique({ where: { slug }, select: { id: true } })) slug = `${base}-${suffix++}`;
    const workspace = await tx.workspace.create({ data: { name: parsed.data.name, slug } });
    await tx.workspaceMember.create({ data: { workspaceId: workspace.id, userId: req.userId!, role: "OWNER" } });
    return workspace;
  });
  res.status(201).json({ success: true, data: { ...created, role: "OWNER" } });
}
async function switchWorkspace(req: Request, res: Response) {
  const id = z.string().uuid().safeParse(req.params.id);
  if (!id.success) { res.status(400).json({ success: false, error: { code: "VALIDATION_ERROR", message: "Invalid workspace." } }); return; }
  const membership = await prisma.workspaceMember.findUnique({ where: { workspaceId_userId: { workspaceId: id.data, userId: req.userId! } }, include: { workspace: true } });
  if (!membership) { res.status(404).json({ success: false, error: { code: "WORKSPACE_NOT_FOUND", message: "Workspace not found." } }); return; }
  await prisma.session.update({ where: { id: req.sessionId! }, data: { activeWorkspaceId: id.data } });
  res.json({ success: true, data: { ...membership.workspace, role: membership.role } });
}
async function renameWorkspace(req: Request, res: Response) {
  const parsed = nameSchema.safeParse(req.body); const member = await activeWorkspace(req);
  if (!parsed.success) { res.status(400).json({ success: false, error: { code: "VALIDATION_ERROR", message: "Enter a valid workspace name." } }); return; }
  if (!member || !canManage(member.role)) { res.status(403).json({ success: false, error: { code: "FORBIDDEN", message: "Workspace administrator access required." } }); return; }
  const workspace = await prisma.workspace.update({ where: { id: member.workspaceId }, data: { name: parsed.data.name } });
  res.json({ success: true, data: workspace });
}
async function overview(req: Request, res: Response) {
  const member = await activeWorkspace(req);
  if (!member) { res.json({ success: true, data: { workspace: null, projects: 0, deployments: 0, succeeded: 0, failed: 0, recentDeployments: [] } }); return; }
  const [projects, deployments, succeeded, failed, recentDeployments] = await Promise.all([
    prisma.project.count({ where: { workspaceId: member.workspaceId } }),
    prisma.deployment.count({ where: { workspaceId: member.workspaceId } }),
    prisma.deployment.count({ where: { workspaceId: member.workspaceId, status: "SUCCEEDED" } }),
    prisma.deployment.count({ where: { workspaceId: member.workspaceId, status: "FAILED" } }),
    prisma.deployment.findMany({ where: { workspaceId: member.workspaceId }, orderBy: { createdAt: "desc" }, take: 8, select: { id: true, status: true, environment: true, createdAt: true, project: { select: { name: true } } } }),
  ]);
  res.json({ success: true, data: { workspace: { id: member.workspaceId, name: member.workspace.name, role: member.role }, projects, deployments, succeeded, failed, recentDeployments } });
}
async function listDeployments(req: Request, res: Response) {
  const member = await activeWorkspace(req);
  const data = member ? await prisma.deployment.findMany({ where: { workspaceId: member.workspaceId }, orderBy: { createdAt: "desc" }, take: 100, select: { id: true, status: true, environment: true, triggerSource: true, createdAt: true, startedAt: true, finishedAt: true, failureCode: true, project: { select: { id: true, name: true } } } }) : [];
  res.json({ success: true, data });
}
async function listTeam(req: Request, res: Response) {
  const member = await activeWorkspace(req);
  if (!member) { res.json({ success: true, data: [] }); return; }
  const members = await prisma.workspaceMember.findMany({ where: { workspaceId: member.workspaceId }, orderBy: { createdAt: "asc" }, select: { userId: true, role: true, createdAt: true } });
  const users = await prisma.user.findMany({ where: { id: { in: members.map(({ userId }) => userId) } }, select: { id: true, name: true, email: true, imageUrl: true } });
  const userById = new Map(users.map((user) => [user.id, user]));
  res.json({ success: true, data: members.flatMap(({ userId, role, createdAt }) => { const user = userById.get(userId); return user ? [{ ...user, role, joinedAt: createdAt }] : []; }) });
}
async function logsSummary(req: Request, res: Response) {
  const member = await activeWorkspace(req);
  const count = member ? await prisma.deploymentLog.count({ where: { workspaceId: member.workspaceId } }) : 0;
  res.json({ success: true, data: { count, contentAvailable: false, reason: "Log content is hidden until a secret redaction pipeline is configured." } });
}
async function listProjects(req: Request, res: Response) {
  const member = await activeWorkspace(req); if (!member) { res.json({ success: true, data: [] }); return; }
  const q = typeof req.query.q === "string" ? req.query.q.trim().slice(0, 100) : "";
  const data = await prisma.project.findMany({ where: { workspaceId: member.workspaceId, ...(q ? { OR: [{ name: { contains: q, mode: "insensitive" } }, { description: { contains: q, mode: "insensitive" } }] } : {}) }, orderBy: { updatedAt: "desc" }, include: { repository: { select: { fullName: true, selectedBranch: true, defaultBranch: true, language: true, isPrivate: true } }, deployments: { orderBy: { createdAt: "desc" }, take: 1, select: { id: true, status: true, createdAt: true } }, _count: { select: { deployments: true } } } });
  res.json({ success: true, data });
}
async function createProject(req: Request, res: Response) {
  const member = await activeWorkspace(req); const parsed = projectSchema.safeParse(req.body);
  if (!parsed.success) { res.status(400).json({ success: false, error: { code: "VALIDATION_ERROR", message: "Enter a valid project name and description." } }); return; }
  if (!member || !canManage(member.role)) { res.status(403).json({ success: false, error: { code: "FORBIDDEN", message: "Workspace administrator access required." } }); return; }
  const base = slugify(parsed.data.name); let slug = base; let suffix = 2;
  while (await prisma.project.findUnique({ where: { workspaceId_slug: { workspaceId: member.workspaceId, slug } }, select: { id: true } })) slug = `${base}-${suffix++}`;
  const project = await prisma.project.create({ data: { workspaceId: member.workspaceId, name: parsed.data.name, description: parsed.data.description || null, slug } });
  res.status(201).json({ success: true, data: project });
}
async function projectById(req: Request, res: Response) {
  const member = await activeWorkspace(req); const id = z.string().uuid().safeParse(req.params.id);
  if (!id.success || !member) { res.status(404).json({ success: false, error: { code: "PROJECT_NOT_FOUND", message: "Project not found." } }); return; }
  const project = await prisma.project.findFirst({
    where: { id: id.data, workspaceId: member.workspaceId },
    include: projectDetailsInclude,
  });
  if (!project) { res.status(404).json({ success: false, error: { code: "PROJECT_NOT_FOUND", message: "Project not found." } }); return; }
  if (req.method === "GET") { res.json({ success: true, data: project }); return; }
  if (!canManage(member.role)) { res.status(403).json({ success: false, error: { code: "FORBIDDEN", message: "Workspace administrator access required." } }); return; }
  if (req.method === "DELETE") { await prisma.project.delete({ where: { id: project.id } }); res.status(204).send(); return; }
  const parsed = projectSchema.safeParse(req.body);
  if (!parsed.success) { res.status(400).json({ success: false, error: { code: "VALIDATION_ERROR", message: "Enter a valid project name and description." } }); return; }
  await prisma.project.update({ where: { id: project.id }, data: { name: parsed.data.name, description: parsed.data.description || null } });
  const updated = await prisma.project.findFirst({
    where: { id: project.id, workspaceId: member.workspaceId },
    include: projectDetailsInclude,
  });
  res.json({ success: true, data: updated });
}

export const workspaceRouter = Router();
workspaceRouter.use(requireAuth);
workspaceRouter.get("/workspaces", listWorkspaces);
workspaceRouter.post("/workspaces", requireCsrf, createWorkspace);
workspaceRouter.post("/workspaces/:id/activate", requireCsrf, switchWorkspace);
workspaceRouter.patch("/workspaces/current", requireCsrf, renameWorkspace);
workspaceRouter.get("/overview", overview);
workspaceRouter.get("/deployments", listDeployments);
workspaceRouter.get("/team", listTeam);
workspaceRouter.get("/logs/summary", logsSummary);
workspaceRouter.get("/projects", listProjects);
workspaceRouter.post("/projects", requireCsrf, createProject);
workspaceRouter.get("/projects/:id", projectById);
workspaceRouter.patch("/projects/:id", requireCsrf, projectById);
workspaceRouter.delete("/projects/:id", requireCsrf, projectById);
