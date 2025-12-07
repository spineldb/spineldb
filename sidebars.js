/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */

// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  tutorialSidebar: [
    {
      type: 'doc',
      id: 'README',
      label: 'Welcome',
    },
    {
      type: 'doc',
      id: 'command-reference',
      label: 'Command Reference',
    },
    {
      type: 'category',
      label: 'Chapter 1: Getting Started',
      items: [
        {
          type: 'doc',
          id: 'installation-and-setup',
        },
      ],
    },
    {
      type: 'category',
      label: 'Chapter 2: Core Data Types',
      items: [
        {
          type: 'doc',
          id: 'core-data-types',
        },
      ],
    },
    {
      type: 'category',
      label: 'Chapter 3: Native Data Structures',
      items: [
        {
          type: 'doc',
          id: 'native-json',
        },
        {
          type: 'doc',
          id: 'geospatial',
        },
        {
          type: 'doc',
          id: 'bloom-filter',
        },
        {
          type: 'doc',
          id: 'hyperloglog',
        },
      ],
    },
    {
      type: 'category',
      label: 'Chapter 4: The Intelligent Caching Engine',
      items: [
        {
          type: 'doc',
          id: 'caching',
        },
        {
          type: 'category',
          label: 'Caching Guides',
          items: [
            {
              type: 'doc',
              id: 'caching/manual-caching-swr',
            },
            {
              type: 'doc',
              id: 'caching/declarative-caching-proxy',
            },
            {
              type: 'doc',
              id: 'caching/tag-based-invalidation',
            },
            {
              type: 'doc',
              id: 'caching/on-disk-caching',
            },
            {
              type: 'doc',
              id: 'caching/content-negotiation-vary',
            },
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Chapter 5: High Availability & Scalability',
      items: [
        {
          type: 'doc',
          id: 'replication',
        },
        {
          type: 'doc',
          id: 'clustering',
        },
        {
          type: 'doc',
          id: 'warden-failover',
        },
      ],
    },
    {
      type: 'category',
      label: 'Chapter 6: Advanced Features',
      items: [
        {
          type: 'doc',
          id: 'security-acl',
        },
        {
          type: 'doc',
          id: 'lua-scripting',
        },
        {
          type: 'doc',
          id: 'transactions',
        },
        {
          type: 'doc',
          id: 'pubsub',
        },
      ],
    },
    {
      type: 'category',
      label: 'Chapter 7: Operations & Monitoring',
      items: [
        {
          type: 'doc',
          id: 'introspection-and-monitoring',
        },
        {
          type: 'doc',
          id: 'persistence-and-backup',
        },
        {
          type: 'doc',
          id: 'troubleshooting',
        },
      ],
    },
  ],
};

module.exports = sidebars;

