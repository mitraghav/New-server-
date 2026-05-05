// save as server.js
// npm install express ws axios fca-mafiya uuid
 
const fs = require('fs');
const express = require('express');
const wiegine = require('fca-mafiya');
const WebSocket = require('ws');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
 
// Initialize Express app
const app = express();
const PORT = process.env.PORT || 5676;
 
// Store active tasks - Persistent storage simulation
const TASKS_FILE = 'active_tasks.json';
const COOKIES_DIR = 'cookies';
 
// Ensure directories exist
if (!fs.existsSync(COOKIES_DIR)) {
    fs.mkdirSync(COOKIES_DIR, { recursive: true });
}
 
// Load persistent tasks
function loadTasks() {
    try {
        if (fs.existsSync(TASKS_FILE)) {
            const data = fs.readFileSync(TASKS_FILE, 'utf8');
            const tasksData = JSON.parse(data);
            const tasks = new Map();
            
            for (let [taskId, taskData] of Object.entries(tasksData)) {
                const task = new Task(taskId, taskData.userData);
                task.config = taskData.config;
                task.messageData = taskData.messageData;
                task.stats = taskData.stats;
                task.logs = taskData.logs || [];
                task.config.running = true; // Auto-restart
                tasks.set(taskId, task);
                
                console.log(`ðŸŒ¹ Reloaded persistent task: ${taskId}`);
                
                // Auto-restart the task
                setTimeout(() => {
                    if (task.config.running) {
                        task.start();
                    }
                }, 5000);
            }
            
            return tasks;
        }
    } catch (error) {
        console.error('Error loading tasks:', error);
    }
    return new Map();
}
 
// Save tasks persistently
function saveTasks() {
    try {
        const tasksData = {};
        for (let [taskId, task] of activeTasks.entries()) {
            if (task.config.running) { // Only save running tasks
                tasksData[taskId] = {
                    userData: task.userData,
                    config: { ...task.config, api: null }, // Remove api reference
                    messageData: task.messageData,
                    stats: task.stats,
                    logs: task.logs.slice(0, 50) // Keep recent logs only
                };
            }
        }
        fs.writeFileSync(TASKS_FILE, JSON.stringify(tasksData, null, 2));
    } catch (error) {
        console.error('Error saving tasks:', error);
    }
}
 
// Auto-save tasks every 30 seconds
setInterval(saveTasks, 30000);
 
// Auto-restart mechanism
function setupAutoRestart() {
    // Check every minute for stuck tasks
    setInterval(() => {
        for (let [taskId, task] of activeTasks.entries()) {
            if (task.config.running && !task.healthCheck()) {
                console.log(`ðŸŒ¹ Auto-restarting stuck task: ${taskId}`);
                task.restart();
            }
        }
    }, 60000);
}
 
let activeTasks = loadTasks();
 
// Enhanced Task management class with auto-recovery
class Task {
    constructor(taskId, userData) {
        this.taskId = taskId;
        this.userData = userData;
        this.config = {
            prefix: '',
            delay: userData.delay || 5,
            running: false,
            api: null,
            repeat: true,
            lastActivity: Date.now(),
            restartCount: 0,
            maxRestarts: 1000 // Unlimited restarts essentially
        };
        this.messageData = {
            threadID: userData.threadID,
            messages: [],
            currentIndex: 0,
            loopCount: 0
        };
        this.stats = {
            sent: 0,
            failed: 0,
            activeCookies: 0,
            loops: 0,
            restarts: 0,
            lastSuccess: null
        };
        this.logs = [];
        this.retryCount = 0;
        this.maxRetries = 50;
        this.initializeMessages(userData.messageContent, userData.hatersName, userData.lastHereName);
    }
 
    initializeMessages(messageContent, hatersName, lastHereName) {
        this.messageData.messages = messageContent
            .split('\n')
            .map(line => line.replace(/\r/g, '').trim())
            .filter(line => line.length > 0)
            .map(message => `${hatersName} ${message} ${lastHereName}`);
        
        this.addLog(`Loaded ${this.messageData.messages.length} formatted messages`);
    }
 
    addLog(message, messageType = 'info') {
        const logEntry = {
            time: new Date().toLocaleTimeString('en-IN'),
            message: message,
            type: messageType
        };
        this.logs.unshift(logEntry);
        if (this.logs.length > 100) {
            this.logs = this.logs.slice(0, 100);
        }
        
        this.config.lastActivity = Date.now();
        broadcastToTask(this.taskId, {
            type: 'log',
            message: message,
            messageType: messageType
        });
    }
 
    healthCheck() {
        // If no activity for 5 minutes, consider stuck
        return Date.now() - this.config.lastActivity < 300000;
    }
 
    async start() {
        if (this.config.running) {
            this.addLog('Task is already running', 'info');
            return true;
        }
 
        this.config.running = true;
        this.retryCount = 0;
        
        try {
            const cookiePath = `${COOKIES_DIR}/cookie_${this.taskId}.txt`;
            fs.writeFileSync(cookiePath, this.userData.cookieContent);
            this.addLog('Cookie content saved', 'success');
        } catch (err) {
            this.addLog(`Failed to save cookie: ${err.message}`, 'error');
            this.config.running = false;
            return false;
        }
 
        if (this.messageData.messages.length === 0) {
            this.addLog('No messages found in the file', 'error');
            this.config.running = false;
            return false;
        }
 
        this.addLog(`Starting task with ${this.messageData.messages.length} messages`);
        
        return this.initializeBot();
    }
 
    initializeBot() {
        return new Promise((resolve) => {
            wiegine.login(this.userData.cookieContent, { 
                logLevel: "silent",
                forceLogin: true,
                selfListen: false
            }, (err, api) => {
                if (err || !api) {
                    this.addLog(`Login failed: ${err ? err.message : 'Unknown error'}`, 'error');
                    
                    // Auto-retry login
                    if (this.retryCount < this.maxRetries) {
                        this.retryCount++;
                        this.addLog(`Auto-retry login attempt ${this.retryCount}/${this.maxRetries} in 30 seconds...`, 'info');
                        
                        setTimeout(() => {
                            this.initializeBot();
                        }, 30000);
                    } else {
                        this.addLog('Max login retries reached. Task paused.', 'error');
                        this.config.running = false;
                    }
                    
                    resolve(false);
                    return;
                }
 
                this.config.api = api;
                this.stats.activeCookies = 1;
                this.retryCount = 0;
                this.addLog('Logged in successfully', 'success');
                
                // Enhanced error handling for API
                this.setupApiErrorHandling(api);
                
                this.getGroupInfo(api, this.messageData.threadID);
                
                this.sendNextMessage(api);
                resolve(true);
            });
        });
    }
 
    setupApiErrorHandling(api) {
        // Handle various API errors gracefully
        if (api && typeof api.listen === 'function') {
            try {
                api.listen((err, event) => {
                    if (err) {
                        // Silent error handling - no user disruption
                        console.log(`ðŸŒ¹ Silent API error handled for task ${this.taskId}`);
                    }
                });
            } catch (e) {
                // Silent catch - no disruption
            }
        }
    }
 
    getGroupInfo(api, threadID) {
        try {
            if (api && typeof api.getThreadInfo === 'function') {
                api.getThreadInfo(threadID, (err, info) => {
                    if (!err && info) {
                        this.addLog(`Target: ${info.name || 'Unknown'} (ID: ${threadID})`, 'info');
                    }
                });
            }
        } catch (e) {
            // Silent error - group info not critical
        }
    }
 
    sendNextMessage(api) {
        if (!this.config.running || !api) {
            return;
        }
 
        // Message loop management
        if (this.messageData.currentIndex >= this.messageData.messages.length) {
            this.messageData.loopCount++;
            this.stats.loops = this.messageData.loopCount;
            this.addLog(`Loop #${this.messageData.loopCount} completed. Restarting.`, 'info');
            this.messageData.currentIndex = 0;
        }
 
        const message = this.messageData.messages[this.messageData.currentIndex];
        const currentIndex = this.messageData.currentIndex;
        const totalMessages = this.messageData.messages.length;
 
        // Enhanced send with multiple fallbacks
        this.sendMessageWithRetry(api, message, currentIndex, totalMessages);
    }
 
    sendMessageWithRetry(api, message, currentIndex, totalMessages, retryAttempt = 0) {
        if (!this.config.running) return;
 
        const maxSendRetries = 10;
        
        try {
            api.sendMessage(message, this.messageData.threadID, (err) => {
                const timestamp = new Date().toLocaleTimeString('en-IN');
                
                if (err) {
                    this.stats.failed++;
                    
                    if (retryAttempt < maxSendRetries) {
                        this.addLog(`ðŸ”„ RETRY ${retryAttempt + 1}/${maxSendRetries} | Message ${currentIndex + 1}/${totalMessages}`, 'info');
                        
                        setTimeout(() => {
                            this.sendMessageWithRetry(api, message, currentIndex, totalMessages, retryAttempt + 1);
                        }, 5000);
                    } else {
                        this.addLog(`âŒ FAILED after ${maxSendRetries} retries | ${timestamp} | Message ${currentIndex + 1}/${totalMessages}`, 'error');
                        this.messageData.currentIndex++;
                        this.scheduleNextMessage(api);
                    }
                } else {
                    this.stats.sent++;
                    this.stats.lastSuccess = Date.now();
                    this.retryCount = 0; // Reset retry count on success
                    this.addLog(`âœ… SENT | ${timestamp} | Message ${currentIndex + 1}/${totalMessages} | Loop ${this.messageData.loopCount + 1}`, 'success');
                    
                    this.messageData.currentIndex++;
                    this.scheduleNextMessage(api);
                }
            });
        } catch (sendError) {
            // Critical send error - restart the bot
            this.addLog(`ðŸš¨ CRITICAL: Send error - restarting bot: ${sendError.message}`, 'error');
            this.restart();
        }
    }
 
    scheduleNextMessage(api) {
        if (!this.config.running) return;
 
        setTimeout(() => {
            try {
                this.sendNextMessage(api);
            } catch (e) {
                this.addLog(`ðŸš¨ Error in message scheduler: ${e.message}`, 'error');
                this.restart();
            }
        }, this.config.delay * 1000);
    }
 
    restart() {
        this.addLog('ðŸ”„ RESTARTING TASK...', 'info');
        this.stats.restarts++;
        this.config.restartCount++;
        
        // Cleanup existing API
        if (this.config.api) {
            try {
                // LOGOUT NAHI KARENGE - SIRF API NULL KARENGE
                // if (typeof this.config.api.logout === 'function') {
                //     this.config.api.logout(); // YE LINE COMMENT KARDI
                // }
            } catch (e) {
                // Silent logout
            }
            this.config.api = null;
        }
        
        this.stats.activeCookies = 0;
        
        // Restart after short delay
        setTimeout(() => {
            if (this.config.running && this.config.restartCount <= this.config.maxRestarts) {
                this.initializeBot();
            } else if (this.config.restartCount > this.config.maxRestarts) {
                this.addLog('ðŸš¨ MAX RESTARTS REACHED - Task stopped', 'error');
                this.config.running = false;
            }
        }, 10000);
    }
 
    stop() {
        console.log(`ðŸŒ¹ Stopping task: ${this.taskId}`);
        this.config.running = false;
        
        // IMPORTANT: LOGOUT NAHI KARENGE - SIRF RUNNING FLAG FALSE KARENGE
        // if (this.config.api) {
        //     try {
        //         if (typeof this.config.api.logout === 'function') {
        //             this.config.api.logout(); // YE LINE COMMENT KARDI
        //         }
        //     } catch (e) {
        //         // ignore logout errors
        //     }
        //     this.config.api = null;
        // }
        
        this.stats.activeCookies = 0;
        this.addLog('ðŸ›‘ Task stopped by user - ID remains logged in', 'info');
        this.addLog('ðŸ”‘ You can use same cookies again without relogin', 'info');
        
        try {
            const cookiePath = `${COOKIES_DIR}/cookie_${this.taskId}.txt`;
            if (fs.existsSync(cookiePath)) {
                fs.unlinkSync(cookiePath);
            }
        } catch (e) {
            // ignore file deletion errors
        }
        
        // Remove from persistent storage
        saveTasks();
        
        return true;
    }
 
    getDetails() {
        return {
            taskId: this.taskId,
            sent: this.stats.sent,
            failed: this.stats.failed,
            activeCookies: this.stats.activeCookies,
            loops: this.stats.loops,
            restarts: this.stats.restarts,
            logs: this.logs,
            running: this.config.running,
            uptime: this.config.lastActivity ? Date.now() - this.config.lastActivity : 0
        };
    }
}
 
// Global error handlers for uninterrupted operation
process.on('uncaughtException', (error) => {
    console.log('ðŸŒ¹ Global error handler caught exception:', error.message);
    // Don't exit - keep running
});
 
process.on('unhandledRejection', (reason, promise) => {
    console.log('ðŸŒ¹ Global handler caught rejection at:', promise, 'reason:', reason);
    // Don't exit - keep running
});
 
// WebSocket broadcast functions
function broadcastToTask(taskId, message) {
    if (!wss) return;
    
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN && client.taskId === taskId) {
            try {
                client.send(JSON.stringify(message));
            } catch (e) {
                // ignore
            }
        }
    });
}
 
// HTML Control Panel - WEBSOCKET STATUS REMOVED
const htmlControlPanel = `
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>JACKSON MULTI-USER MESSAGING SYSTEM</title>
<style>
  * {
    box-sizing: border-box;
    font-family: Inter, system-ui, Arial, sans-serif;
  }
  html, body {
    height: 100%;
    margin: 0;
    background: #000;
    color: #cfcfcf;
  }
  
  body {
    background: linear-gradient(135deg, #1a0033 0%, #330033 50%, #660066 100%);
    overflow-y: auto;
    position: relative;
  }
  
  .rain-background {
    position: fixed;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    pointer-events: none;
    z-index: -1;
    opacity: 0.3;
  }
  
  .raindrop {
    position: absolute;
    width: 2px;
    height: 15px;
    background: linear-gradient(transparent, #ff66cc, transparent);
    animation: fall linear infinite;
  }
  
  @keyframes fall {
    to {
      transform: translateY(100vh);
    }
  }
  
  header {
    padding: 18px 22px;
    display: flex;
    align-items: center;
    gap: 16px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    background: linear-gradient(90deg, rgba(102, 0, 153, 0.6), rgba(153, 0, 102, 0.3));
    backdrop-filter: blur(4px);
  }
  
  header h1 {
    margin: 0;
    font-size: 18px;
    color: #ff66cc;
    text-shadow: 0 0 8px rgba(255, 102, 204, 0.6);
  }
  
  header .sub {
    font-size: 12px;
    color: #cc99cc;
    margin-left: auto;
  }
 
  .container {
    max-width: 1200px;
    margin: 20px auto;
    padding: 20px;
  }
  
  .panel {
    background: rgba(40, 0, 60, 0.7);
    border: 1px solid rgba(255, 102, 204, 0.2);
    padding: 16px;
    border-radius: 10px;
    margin-bottom: 16px;
    box-shadow: 0 8px 30px rgba(102, 0, 153, 0.5);
  }
 
  label {
    font-size: 13px;
    color: #ff99cc;
  }
  
  .row {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
  }
  
  .full {
    grid-column: 1 / 3;
  }
  
  input[type="text"], input[type="number"], textarea, select, .fake-file {
    width: 100%;
    padding: 10px;
    border-radius: 8px;
    border: 1px solid rgba(255, 102, 204, 0.3);
    background: rgba(60, 0, 80, 0.6);
    color: #ffccff;
    outline: none;
    transition: all 0.3s ease;
    font-size: 14px;
  }
  
  input:focus, textarea:focus {
    box-shadow: 0 0 15px rgba(255, 102, 204, 0.8);
    border-color: #ff66cc;
    transform: scale(1.02);
  }
 
  .fake-file {
    display: flex;
    align-items: center;
    gap: 8px;
    cursor: pointer;
  }
  
  input[type=file] {
    display: block;
  }
  
  .controls {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    margin-top: 12px;
  }
 
  button {
    padding: 10px 14px;
    border-radius: 8px;
    border: 0;
    cursor: pointer;
    background: linear-gradient(45deg, #ff66cc, #cc00cc);
    color: white;
    font-weight: 600;
    box-shadow: 0 6px 18px rgba(255, 102, 204, 0.4);
    transition: all 0.3s ease;
  }
  
  button:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 20px rgba(255, 102, 204, 0.6);
  }
  
  button:disabled {
    opacity: .5;
    cursor: not-allowed;
    transform: none;
  }
 
  .log {
    height: 300px;
    overflow: auto;
    background: #1a0033;
    border-radius: 8px;
    padding: 12px;
    font-family: monospace;
    color: #ff66cc;
    border: 1px solid rgba(255, 102, 204, 0.2);
    font-size: 12px;
  }
  
  .task-id-box {
    background: linear-gradient(45deg, #660066, #990066);
    padding: 15px;
    border-radius: 10px;
    margin: 10px 0;
    border: 2px solid #ff66cc;
    text-align: center;
    animation: glow 2s infinite alternate;
  }
  
  @keyframes glow {
    from {
      box-shadow: 0 0 10px #ff66cc;
    }
    to {
      box-shadow: 0 0 20px #ff00ff, 0 0 30px #ff66cc;
    }
  }
  
  .task-id {
    font-size: 18px;
    font-weight: bold;
    color: #ffffff;
    word-break: break-all;
  }
  
  .stats {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr 1fr;
    gap: 10px;
    margin: 10px 0;
  }
  
  .stat-box {
    background: rgba(80, 0, 120, 0.6);
    padding: 10px;
    border-radius: 8px;
    text-align: center;
    border: 1px solid rgba(255, 102, 204, 0.3);
  }
  
  .stat-value {
    font-size: 20px;
    font-weight: bold;
    color: #ff66cc;
  }
  
  .stat-label {
    font-size: 12px;
    color: #cc99cc;
  }
  
  .message-item {
    border-left: 3px solid #ff66cc;
    padding-left: 10px;
    margin: 5px 0;
    background: rgba(60, 0, 80, 0.3);
    padding: 8px;
    border-radius: 4px;
  }
  
  .success {
    color: #66ff66;
  }
  
  .error {
    color:
