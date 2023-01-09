function state = getJobStateFcn(cluster, job, state)
%GETJOBSTATEFCN Gets the state of a job from HTCondor
%
% Set your cluster's PluginScriptsLocation to the parent folder of this
% function to run it when you query the state of a job.

% Copyright 2010-2022 The MathWorks, Inc.

% Store the current filename for the errors, warnings and
% dctSchedulerMessages
currFilename = mfilename;
if ~isa(cluster, 'parallel.Cluster')
    error('parallelexamples:GenericHTCondor:SubmitFcnError', ...
        'The function %s is for use with clusters created using the parcluster command.', currFilename)
end

% Get the information about the actual cluster used
data = cluster.getJobClusterData(job);
if isempty(data)
    % This indicates that the job has not been submitted, so just return
    dctSchedulerMessage(1, '%s: Job cluster data was empty for job with ID %d.', currFilename, job.ID);
    return
end

% Shortcut if the job state is already finished or failed
jobInTerminalState = strcmp(state, 'finished') || strcmp(state, 'failed');
if jobInTerminalState
    if cluster.HasSharedFilesystem
        return
    end
    try
        hasDoneLastMirror = data.HasDoneLastMirror;
    catch err
        ex = MException('parallelexamples:GenericHTCondor:FailedToRetrieveRemoteParameters', ...
            'Failed to retrieve remote parameters from the job cluster data.');
        ex = ex.addCause(err);
        throw(ex);
    end
    % Can only shortcut here if we've already done the last mirror
    if hasDoneLastMirror
        return
    end
end

[schedulerIDs, numSubmittedTasks] = getSimplifiedSchedulerIDsForJob(job);

% Get the top level job state from condor_q
jobList = sprintf('''%s'' ', schedulerIDs{:});
commandToRun = sprintf('condor_q -long -attributes JobStatus %s ; condor_history -long -attributes JobStatus %s', jobList, jobList);
dctSchedulerMessage(4, '%s: Querying cluster for job state using command:\n\t%s', currFilename, commandToRun);

try
    % We will ignore the status returned from the state command because
    % a non-zero status is returned if the job no longer exists
    [~, cmdOut] = runSchedulerCommand(cluster, commandToRun);
catch err
    ex = MException('parallelexamples:GenericHTCondor:FailedToGetJobState', ...
        'Failed to get job state from cluster.');
    ex = ex.addCause(err);
    throw(ex);
end

clusterState = iExtractJobState(cmdOut, numSubmittedTasks);
dctSchedulerMessage(6, '%s: State %s was extracted from cluster output.', currFilename, clusterState);

% If we could determine the cluster's state, we'll use that, otherwise
% stick with MATLAB's job state.
if ~strcmp(clusterState, 'unknown')
    state = clusterState;
end

if ~cluster.HasSharedFilesystem
    % Decide what to do with mirroring based on the cluster's version of job state and whether or not
    % the job is currently being mirrored:
    % If job is not being mirrored, and job is not finished, resume the mirror
    % If job is not being mirrored, and job is finished, do the last mirror
    % If the job is being mirrored, and job is finished, do the last mirror.
    % Otherwise (if job is not finished, and we are mirroring), do nothing
    remoteConnection = getRemoteConnection(cluster);
    isBeingMirrored = remoteConnection.isJobUsingConnection(job.ID);
    isJobFinished = strcmp(state, 'finished') || strcmp(state, 'failed');
    if ~isBeingMirrored && ~isJobFinished
        % resume the mirror
        dctSchedulerMessage(4, '%s: Resuming mirror for job %d.', currFilename, job.ID);
        try
            remoteConnection.resumeMirrorForJob(job);
        catch err
            warning('parallelexamples:GenericHTCondor:FailedToResumeMirrorForJob', ...
                'Failed to resume mirror for job %d.  Your local job files may not be up-to-date.\nReason: %s', ...
                err.getReport);
        end
    elseif isJobFinished
        dctSchedulerMessage(4, '%s: Doing last mirror for job %d.', currFilename, job.ID);
        try
            remoteConnection.doLastMirrorForJob(job);
            % Store the fact that we have done the last mirror so we can shortcut in the future
            data.HasDoneLastMirror = true;
            cluster.setJobClusterData(job, data);
        catch err
            warning('parallelexamples:GenericHTCondor:FailedToDoFinalMirrorForJob', ...
                'Failed to do last mirror for job %d.  Your local job files may not be up-to-date.\nReason: %s', ...
                err.getReport);
        end
    end
end

end

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
function state = iExtractJobState(condor_qOut, numJobs)
% Function to extract the job state from the output of condor_q

numPending  = numel(regexp(condor_qOut, '0|1|5'));
numRunning  = numel(regexp(condor_qOut, '2'));
numFinished = numel(regexp(condor_qOut, '4'));
numFailed   = numel(regexp(condor_qOut, '3|6'));

% If all of the jobs that we asked about have finished, then we know the
% job has finished.
if numFinished == numJobs
    state = 'finished';
    return
end

% Any running indicates that the job is running
if numRunning > 0
    state = 'running';
    return
end

% We know numRunning == 0 so if there are some still pending then the
% job must be queued again, even if there are some finished
if numPending > 0
    state = 'queued';
    return
end

% Deal with any tasks that have failed
if numFailed > 0
    % Set this job to be failed
    state = 'failed';
    return
end

state = 'unknown';
end
