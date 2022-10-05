function jobID = extractJobId(condorSubmitCommandOutput)
% Extracts the job ID from the condor_submit command output for HTCondor

% Copyright 2020-2022 The MathWorks, Inc.

% Output from HTCondor expected to be in the following format:
% X job(s) submitted to cluster 12345

% Trim condor_submit command output for use in debug message
trimmedCommandOutput = strtrim(condorSubmitCommandOutput);

% Ignore anything before or after 'Submitted batch job ###', and extract the numeric value.
searchPattern = '.*submitted to cluster ([0-9]+).*';

% When we match searchPattern, matchedTokens is a single entry cell array containing the jobID.
% Otherwise we failed to match searchPattern, so matchedTokens is an empty cell array.
matchedTokens = regexp(condorSubmitCommandOutput, searchPattern, 'tokens', 'once');

if isempty(matchedTokens)
    % Callers check for error in extracting Job ID using isempty() on return value.
    jobID = '';
    dctSchedulerMessage(0, '%s: Failed to extract Job ID from condor_submit output: \n\t%s', mfilename, trimmedCommandOutput);
else
    jobID = matchedTokens{1};
    dctSchedulerMessage(0, '%s: Job ID %s was extracted from condor_submit output: \n\t%s', mfilename, jobID, trimmedCommandOutput);
end

end
