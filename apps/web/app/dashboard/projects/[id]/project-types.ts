export type ProjectDeployment = {
  id: string;
  status: string;
  environment: string;
  triggerSource: string;
  requestedBranch: string | null;
  commitSha: string | null;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  failureCode: string | null;
};

export type ProjectEnvironmentVariable = {
  id: string;
  key: string;
  environment: string;
  createdAt: string;
  updatedAt: string;
};

export type ProjectRepository = {
  id: string;
  provider: string;
  externalId: string;
  name: string;
  fullName: string | null;
  defaultBranch: string | null;
  selectedBranch: string | null;
  language: string | null;
  isPrivate: boolean;
  createdAt: string;
  updatedAt: string;
  installation: {
    accountLogin: string;
    accountType: string;
    status: string;
    repositorySelection: string;
  } | null;
};

export type Project = {
  id: string;
  name: string;
  slug: string;
  description: string | null;
  createdAt: string;
  updatedAt: string;
  repository: ProjectRepository | null;
  deployments: ProjectDeployment[];
  environmentVariables: ProjectEnvironmentVariable[];
  _count: { deployments: number; environmentVariables: number };
};
