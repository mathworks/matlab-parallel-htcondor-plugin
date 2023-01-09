function submitString = getSubmitString(submitDescriptionFileName)
%GETSUBMITSTRING Gets the correct condor_submit command for a HTCondor cluster

% Copyright 2020-2022 The MathWorks, Inc.

submitString = sprintf('condor_submit %s', ...
    submitDescriptionFileName);

end
