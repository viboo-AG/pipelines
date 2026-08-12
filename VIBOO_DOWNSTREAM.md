# Viboo downstream release

This branch contains Viboo-specific changes on top of Kubeflow Pipelines.

The changes are maintained internally and have not been submitted to the upstream repository.

## Base

- Upstream repository: `kubeflow/pipelines`
- Upstream tag: `2.16.0`
- Upstream commit: `e4ebca310f404dac306e16bc880de12bbbf63b95`

## Downstream concerns

- `fix(backend): increase launcher memory allocation`
- `fix(backend): retry tasks after all failure types`

The launcher memory and retry policy changes are separate because they have different runtime effects and may be resolved by different upstream releases.

## Verification

Pushes to `release/**` run `.github/workflows/viboo-downstream-ci.yml`.

The workflow checks Go formatting, runs compiler unit tests, builds the API server, and stores the binary as a GitHub Actions artifact.

The deployable API server image is still built with the Dockerfile in `viboo-kubeflow`.

## Upgrade procedure

1. Create a new release branch from the exact new upstream tag.
2. Evaluate launcher memory independently from retry behavior.
3. Omit a concern if the new upstream release provides equivalent behavior.
4. Cherry-pick each remaining concern separately and resolve any semantic changes.
5. Run the downstream CI workflow and build the deployable API server image.
6. Deploy the image to DEV by immutable digest.
7. Promote the same image digest to PROD only after DEV verification.
