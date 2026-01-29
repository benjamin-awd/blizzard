// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	site: 'https://benjamin-awd.github.io',
	base: '/blizzard',
	integrations: [
		starlight({
			title: '❄️ Blizzard',
			social: [
			],
			sidebar: [
				{
					label: 'Getting Started',
					items: [
						{ label: 'Introduction', slug: 'index' },
					],
				},
				{
					label: 'Architecture',
					items: [
						{ label: 'Pipeline Overview', slug: 'architecture/pipeline' },
						{ label: 'Checkpoints', slug: 'architecture/checkpoint' },
						{ label: 'Storage Backends', slug: 'architecture/storage' },
						{ label: 'Source Processing', slug: 'architecture/source' },
						{ label: 'Sink & Delta Lake', slug: 'architecture/sink' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'Configuration', slug: 'reference/configuration' },
						{ label: 'Error Handling', slug: 'reference/errors' },
						{ label: 'Dead Letter Queue', slug: 'reference/dlq' },
						{ label: 'Metrics', slug: 'reference/metrics' },
						{ label: '🐧 Penguin', slug: 'reference/penguin' },
					],
				},
			],
			editLink: {
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
