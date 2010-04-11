require 'rubygems'
require 'spec/rake/spectask'

task :default => [:clean,:build,:specs]

task :clean do
  `rm ext/mkmf.log ext/redis.o ext/redis.bundle ext/Makefile`
end

task :build do
  `cd ext ; ruby extconf.rb ; make ; cd ..`
end

desc "Run all examples"
Spec::Rake::SpecTask.new('specs') do |t|
  t.spec_files = FileList['specs/*.rb']
end
