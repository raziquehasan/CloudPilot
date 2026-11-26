import { GitHubServiceError, createInstallationOctokit, mapGitHubError } from "./github-app.service.js";

const GITHUB_REQUEST_TIMEOUT_MS = 12_000;

export interface ResolvedCommit {
  sha: string;
  shortSha: string;
  commitMessage: string;
  authorName: string | null;
  url: string;
  verified: boolean | null;
  requestedBranch: string | null;
}

interface CommitListItem {
  sha: string;
  commit: {
    message: string;
    author?: { name?: string | null } | null;
    committer?: { name?: string | null } | null;
    verification?: { verified?: boolean } | null;
  };
  author?: { login?: string } | null;
  html_url: string;
}

export async function resolveImmutableCommit(input: {
  installationId: bigint;
  owner: string;
  repo: string;
  expectedFullName: string;
  requestedBranch: string | null;
  requestedCommit: string | null;
}): Promise<ResolvedCommit> {
  if (!input.requestedBranch && !input.requestedCommit) {
    throw new GitHubServiceError(422, "A branch or commit must be selected; the default branch is never assumed.");
  }

  const octokit = await createInstallationOctokit(input.installationId);
  const request = { request: { timeout: GITHUB_REQUEST_TIMEOUT_MS } };

  try {
    const repository = await octokit.rest.repos.get({ owner: input.owner, repo: input.repo, ...request });
    if (repository.data.full_name.toLowerCase() !== input.expectedFullName.toLowerCase()) {
      throw new GitHubServiceError(409, "The resolved repository does not match the authorized repository.");
    }

    if (input.requestedCommit) {
      const commit = await octokit.rest.repos.getCommit({
        owner: input.owner,
        repo: input.repo,
        ref: input.requestedCommit,
        ...request,
      });
      if (commit.data.sha.toLowerCase() !== input.requestedCommit.toLowerCase()) {
        throw new GitHubServiceError(409, "The resolved commit does not match the requested commit.");
      }
      return mapCommit(commit.data as unknown as CommitListItem, input.requestedBranch);
    }

    const branch = input.requestedBranch!;
    const commits = await octokit.rest.repos.listCommits({
      owner: input.owner,
      repo: input.repo,
      sha: branch,
      per_page: 1,
      ...request,
    });
    const latest = commits.data[0];
    if (!latest) {
      throw new GitHubServiceError(404, "The selected branch has no commits.");
    }
    return mapCommit(latest as unknown as CommitListItem, branch);
  } catch (error) {
    if (error instanceof GitHubServiceError) throw error;
    throw mapGitHubError(error);
  }
}

function mapCommit(commit: CommitListItem, branch: string | null): ResolvedCommit {
  return {
    sha: commit.sha,
    shortSha: commit.sha.slice(0, 7),
    commitMessage: commit.commit.message.split("\n")[0]?.slice(0, 500) ?? "",
    authorName: commit.commit.author?.name ?? commit.commit.committer?.name ?? commit.author?.login ?? null,
    url: commit.html_url,
    verified: commit.commit.verification?.verified ?? null,
    requestedBranch: branch,
  };
}