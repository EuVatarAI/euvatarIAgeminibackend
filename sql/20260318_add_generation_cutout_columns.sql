alter table public.generations
  add column if not exists cutout_path text,
  add column if not exists cutout_url text;
