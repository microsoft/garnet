git rev-list --objects --all |
git cat-file --batch-check="%(objecttype) %(objectname) %(objectsize) %(rest)" |
Where-Object { $_ -match "^blob " } |
ForEach-Object {
    $parts = $_.Split(" ", 4)
    [PSCustomObject]@{
        Size = [int64]$parts[2]
        Hash = $parts[1]
        Path = $parts[3]
    }
} |
Sort-Object Size -Descending |
Select-Object -First 100