/**
 * Memory Bank System
 * A comprehensive memory management system for storing and retrieving project context
 */

class MemoryBank {
  constructor() {
    this.memories = new Map();
    this.categories = new Set();
    this.tags = new Set();
    this.lastUpdated = new Date();
  }

  /**
   * Store a new memory with metadata
   * @param {string} id - Unique identifier for the memory
   * @param {Object} content - The memory content
   * @param {string} category - Category for organization
   * @param {Array<string>} tags - Tags for searchability
   * @param {number} priority - Priority level (1-10, 10 being highest)
   * @param {Date} expiresAt - Optional expiration date
   */
  store(id, content, category = 'general', tags = [], priority = 5, expiresAt = null) {
    const memory = {
      id,
      content,
      category,
      tags: Array.isArray(tags) ? tags : [tags],
      priority,
      createdAt: new Date(),
      lastAccessed: new Date(),
      expiresAt,
      accessCount: 0
    };

    this.memories.set(id, memory);
    this.categories.add(category);
    memory.tags.forEach(tag => this.tags.add(tag));
    this.lastUpdated = new Date();

    return memory;
  }

  /**
   * Retrieve a memory by ID
   * @param {string} id - Memory ID
   * @returns {Object|null} Memory object or null if not found
   */
  retrieve(id) {
    const memory = this.memories.get(id);
    if (memory) {
      memory.lastAccessed = new Date();
      memory.accessCount++;
    }
    return memory;
  }

  /**
   * Search memories by various criteria
   * @param {Object} criteria - Search criteria
   * @returns {Array} Array of matching memories
   */
  search(criteria = {}) {
    let results = Array.from(this.memories.values());

    // Filter by category
    if (criteria.category) {
      results = results.filter(memory => memory.category === criteria.category);
    }

    // Filter by tags
    if (criteria.tags && criteria.tags.length > 0) {
      results = results.filter(memory => 
        criteria.tags.some(tag => memory.tags.includes(tag))
      );
    }

    // Filter by priority
    if (criteria.minPriority) {
      results = results.filter(memory => memory.priority >= criteria.minPriority);
    }

    // Filter by date range
    if (criteria.since) {
      results = results.filter(memory => memory.createdAt >= criteria.since);
    }

    if (criteria.until) {
      results = results.filter(memory => memory.createdAt <= criteria.until);
    }

    // Text search in content
    if (criteria.text) {
      const searchText = criteria.text.toLowerCase();
      results = results.filter(memory => 
        JSON.stringify(memory.content).toLowerCase().includes(searchText)
      );
    }

    // Sort by priority and last accessed
    results.sort((a, b) => {
      if (a.priority !== b.priority) {
        return b.priority - a.priority;
      }
      return b.lastAccessed - a.lastAccessed;
    });

    return results;
  }

  /**
   * Update an existing memory
   * @param {string} id - Memory ID
   * @param {Object} updates - Updates to apply
   * @returns {Object|null} Updated memory or null if not found
   */
  update(id, updates) {
    const memory = this.memories.get(id);
    if (!memory) return null;

    Object.assign(memory, updates, { lastUpdated: new Date() });
    return memory;
  }

  /**
   * Delete a memory
   * @param {string} id - Memory ID
   * @returns {boolean} True if deleted, false if not found
   */
  delete(id) {
    return this.memories.delete(id);
  }

  /**
   * Get all categories
   * @returns {Array} Array of categories
   */
  getCategories() {
    return Array.from(this.categories);
  }

  /**
   * Get all tags
   * @returns {Array} Array of tags
   */
  getTags() {
    return Array.from(this.tags);
  }

  /**
   * Clean up expired memories
   * @returns {number} Number of memories cleaned up
   */
  cleanup() {
    const now = new Date();
    let cleaned = 0;

    for (const [id, memory] of this.memories.entries()) {
      if (memory.expiresAt && memory.expiresAt < now) {
        this.memories.delete(id);
        cleaned++;
      }
    }

    return cleaned;
  }

  /**
   * Export memories to JSON
   * @returns {Object} Exported data
   */
  export() {
    return {
      memories: Array.from(this.memories.entries()),
      categories: Array.from(this.categories),
      tags: Array.from(this.tags),
      lastUpdated: this.lastUpdated,
      version: '1.0.0'
    };
  }

  /**
   * Import memories from JSON
   * @param {Object} data - Imported data
   */
  import(data) {
    if (data.memories) {
      this.memories = new Map(data.memories);
    }
    if (data.categories) {
      this.categories = new Set(data.categories);
    }
    if (data.tags) {
      this.tags = new Set(data.tags);
    }
    if (data.lastUpdated) {
      this.lastUpdated = new Date(data.lastUpdated);
    }
  }

  /**
   * Get memory statistics
   * @returns {Object} Statistics object
   */
  getStats() {
    const memories = Array.from(this.memories.values());
    return {
      totalMemories: memories.length,
      categories: this.categories.size,
      tags: this.tags.size,
      lastUpdated: this.lastUpdated,
      averagePriority: memories.reduce((sum, m) => sum + m.priority, 0) / memories.length || 0,
      mostAccessed: memories.sort((a, b) => b.accessCount - a.accessCount)[0]?.id || null
    };
  }
}

// Create a singleton instance
const memoryBank = new MemoryBank();

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
  module.exports = { MemoryBank, memoryBank };
}

// Make available globally in browser
if (typeof window !== 'undefined') {
  window.MemoryBank = MemoryBank;
  window.memoryBank = memoryBank;
}
