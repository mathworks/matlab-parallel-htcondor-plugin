function envStr = formatEnvironmentString(environmentVariables)
% Build the environment argument for the submit description file

% Copyright 2022 The MathWorks, Inc.

envStr = '"';
for ii = 1:size(environmentVariables, 1)
    envStr = sprintf('%s %s=%s', envStr, environmentVariables{ii,1}, iFormatEnvironmentValue(environmentVariables{ii,2}));
end
envStr = sprintf('%s"', envStr);
end

function str = iFormatEnvironmentValue(value)
str = strrep(value, '"', '""'); % escape double quotes
if contains(str, [" ","'"])
    str = strrep(str, "'", "''"); % escape single quotes
    str = sprintf("'%s'", str); % escape spaces
end
end
