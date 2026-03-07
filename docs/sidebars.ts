import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    'index',
    {
      type: 'category',
      label: 'Getting Started',
      items: [
        'getting-started/installation',
        'getting-started/quick-start',
      ],
    },
    {
      type: 'category',
      label: 'Guides',
      items: [
        'guides/core-concepts',
        'guides/working-with-arrow',
        'guides/querying-data',
        'guides/reading-data',
        'guides/writing-data',
        'guides/object-stores',
        'guides/hive-partitioning',
      ],
    },
  ],
};

export default sidebars;
