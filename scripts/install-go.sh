# 1. Define the version you want
GO_VERSION=1.24.5

# 2. Download the Go tarball
curl -LO https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz

# 3. Remove any previous Go installation
sudo rm -rf /usr/local/go

# 4. Extract the archive to /usr/local
sudo tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz

# 5. Add Go to your PATH (for current shell session)
export PATH=$PATH:/usr/local/go/bin

# 6. Optionally, add to ~/.bashrc or ~/.zshrc for persistence
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
# or for Zsh users:
# echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.zshrc

# 7. Reload shell config (if added to ~/.bashrc)
source ~/.bashrc

# 8. Verify installation
go version
