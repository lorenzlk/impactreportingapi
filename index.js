#!/usr/bin/env node

/**
 * Memory Bank System - Main Entry Point
 * Provides CLI interface and basic functionality
 */

const { memoryBank } = require('./memory-bank');
const fs = require('fs-extra');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

class MemoryBankCLI {
  constructor() {
    this.dataDir = path.join(__dirname, 'data');
    this.memoryFile = path.join(this.dataDir, 'memories.json');
    this.initializeDataDirectory();
    this.loadMemories();
  }

  /**
   * Initialize data directory
   */
  async initializeDataDirectory() {
    try {
      await fs.ensureDir(this.dataDir);
      console.log('✓ Data directory initialized');
    } catch (error) {
      console.error('Failed to initialize data directory:', error.message);
    }
  }

  /**
   * Load memories from file
   */
  async loadMemories() {
    try {
      if (await fs.pathExists(this.memoryFile)) {
        const data = await fs.readJson(this.memoryFile);
        memoryBank.import(data);
        console.log('✓ Memories loaded from file');
      }
    } catch (error) {
      console.error('Failed to load memories:', error.message);
    }
  }

  /**
   * Save memories to file
   */
  async saveMemories() {
    try {
      const data = memoryBank.export();
      await fs.writeJson(this.memoryFile, data, { spaces: 2 });
      console.log('✓ Memories saved to file');
    } catch (error) {
      console.error('Failed to save memories:', error.message);
    }
  }

  /**
   * Add a new memory
   */
  addMemory(content, category = 'general', tags = [], priority = 5) {
    const id = uuidv4();
    const memory = memoryBank.store(id, content, category, tags, priority);
    this.saveMemories();
    console.log(`✓ Memory added with ID: ${id}`);
    return memory;
  }

  /**
   * Search memories
   */
  searchMemories(criteria = {}) {
    const results = memoryBank.search(criteria);
    console.log(`Found ${results.length} memories:`);
    results.forEach((memory, index) => {
      console.log(`${index + 1}. [${memory.category}] ${memory.id}`);
      console.log(`   Priority: ${memory.priority}, Tags: ${memory.tags.join(', ')}`);
      console.log(`   Content: ${JSON.stringify(memory.content, null, 2)}`);
      console.log('');
    });
    return results;
  }

  /**
   * Get memory by ID
   */
  getMemory(id) {
    const memory = memoryBank.retrieve(id);
    if (memory) {
      console.log(`Memory: ${id}`);
      console.log(`Category: ${memory.category}`);
      console.log(`Priority: ${memory.priority}`);
      console.log(`Tags: ${memory.tags.join(', ')}`);
      console.log(`Content: ${JSON.stringify(memory.content, null, 2)}`);
      console.log(`Created: ${memory.createdAt}`);
      console.log(`Last accessed: ${memory.lastAccessed}`);
      console.log(`Access count: ${memory.accessCount}`);
    } else {
      console.log(`Memory not found: ${id}`);
    }
    return memory;
  }

  /**
   * List all categories
   */
  listCategories() {
    const categories = memoryBank.getCategories();
    console.log('Categories:');
    categories.forEach(category => console.log(`- ${category}`));
    return categories;
  }

  /**
   * List all tags
   */
  listTags() {
    const tags = memoryBank.getTags();
    console.log('Tags:');
    tags.forEach(tag => console.log(`- ${tag}`));
    return tags;
  }

  /**
   * Get statistics
   */
  getStats() {
    const stats = memoryBank.getStats();
    console.log('Memory Bank Statistics:');
    console.log(`Total memories: ${stats.totalMemories}`);
    console.log(`Categories: ${stats.categories}`);
    console.log(`Tags: ${stats.tags}`);
    console.log(`Last updated: ${stats.lastUpdated}`);
    console.log(`Average priority: ${stats.averagePriority.toFixed(2)}`);
    console.log(`Most accessed: ${stats.mostAccessed || 'None'}`);
    return stats;
  }

  /**
   * Clean up expired memories
   */
  cleanup() {
    const cleaned = memoryBank.cleanup();
    console.log(`✓ Cleaned up ${cleaned} expired memories`);
    this.saveMemories();
    return cleaned;
  }

  /**
   * Interactive mode
   */
  async interactive() {
    const readline = require('readline');
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout
    });

    console.log('Memory Bank System - Interactive Mode');
    console.log('Type "help" for available commands');

    const askQuestion = (question) => {
      return new Promise((resolve) => {
        rl.question(question, resolve);
      });
    };

    while (true) {
      const input = await askQuestion('\n> ');
      const [command, ...args] = input.trim().split(' ');

      switch (command.toLowerCase()) {
        case 'help':
          console.log(`
Available commands:
  add <content> [category] [tags] [priority] - Add a new memory
  get <id> - Get memory by ID
  search [criteria] - Search memories
  categories - List all categories
  tags - List all tags
  stats - Show statistics
  cleanup - Remove expired memories
  save - Save memories to file
  load - Load memories from file
  exit - Exit the program
          `);
          break;

        case 'add':
          if (args.length === 0) {
            console.log('Usage: add <content> [category] [tags] [priority]');
            break;
          }
          const content = args[0];
          const category = args[1] || 'general';
          const tags = args[2] ? args[2].split(',') : [];
          const priority = parseInt(args[3]) || 5;
          this.addMemory(content, category, tags, priority);
          break;

        case 'get':
          if (args.length === 0) {
            console.log('Usage: get <id>');
            break;
          }
          this.getMemory(args[0]);
          break;

        case 'search':
          const criteria = {};
          if (args.length > 0) {
            criteria.text = args.join(' ');
          }
          this.searchMemories(criteria);
          break;

        case 'categories':
          this.listCategories();
          break;

        case 'tags':
          this.listTags();
          break;

        case 'stats':
          this.getStats();
          break;

        case 'cleanup':
          this.cleanup();
          break;

        case 'save':
          await this.saveMemories();
          break;

        case 'load':
          await this.loadMemories();
          break;

        case 'exit':
          console.log('Goodbye!');
          rl.close();
          return;

        default:
          console.log('Unknown command. Type "help" for available commands.');
      }
    }
  }
}

// CLI execution
if (require.main === module) {
  const cli = new MemoryBankCLI();
  
  // Handle command line arguments
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    // Start interactive mode
    cli.interactive().catch(console.error);
  } else {
    // Handle specific commands
    const [command, ...commandArgs] = args;
    
    switch (command) {
      case 'add':
        if (commandArgs.length === 0) {
          console.log('Usage: node index.js add <content> [category] [tags] [priority]');
          process.exit(1);
        }
        cli.addMemory(commandArgs[0], commandArgs[1], commandArgs[2]?.split(','), parseInt(commandArgs[3]) || 5);
        break;
        
      case 'search':
        cli.searchMemories({ text: commandArgs.join(' ') });
        break;
        
      case 'stats':
        cli.getStats();
        break;
        
      case 'cleanup':
        cli.cleanup();
        break;
        
      default:
        console.log('Unknown command. Available commands: add, search, stats, cleanup');
        process.exit(1);
    }
  }
}

module.exports = MemoryBankCLI;
