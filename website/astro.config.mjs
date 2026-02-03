// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import d2 from 'astro-d2';

// https://astro.build/config
export default defineConfig({
	site: 'https://benjamin-awd.github.io',
	base: '/blizzard',
	integrations: [
		d2(),
		starlight({
			title: '❄️ Blizzard',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/benjamin-awd/blizzard' },
			],
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Introduction', slug: 'index' },
					],
				},
				{
					label: '❄️ Blizzard',
					items: [
						{ label: 'Pipeline Overview', slug: 'architecture/pipeline' },
						{ label: 'Topology', slug: 'architecture/topology' },
						{ label: 'Source Processing', slug: 'architecture/source' },
						{ label: 'Parquet Writer', slug: 'architecture/sink' },
						{ label: 'Storage Backends', slug: 'architecture/storage' },
						{ label: 'Configuration', slug: 'reference/configuration' },
						{ label: 'Error Handling', slug: 'reference/errors' },
						{ label: 'Dead Letter Queue', slug: 'reference/dlq' },
						{ label: 'Metrics', slug: 'reference/metrics' },
					],
				},
				{
					label: '🐧 Penguin',
					items: [
						{ label: 'Overview', slug: 'penguin' },
						{ label: 'Delta Lake Commits', slug: 'penguin/delta-lake' },
						{ label: 'Schema Evolution', slug: 'penguin/schema-evolution' },
						{ label: 'Fault Tolerance', slug: 'penguin/fault-tolerance' },
						{ label: 'Configuration', slug: 'penguin/configuration' },
					],
				},
			],
			editLink: {
				baseUrl: 'https://github.com/benjamin-awd/blizzard/edit/main/website/',
			},
			head: [
				{
					tag: 'meta',
					attrs: {
						property: 'og:image',
						content: 'https://benjamin-awd.github.io/blizzard/og.png',
					},
				},
			],
		}),
	],
});
