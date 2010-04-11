require 'mkmf'

success = true

if !have_library('redis', 'Module_new') && !find_library('redis', 'Module_new', './lib')
  puts 'libredis is missing!'
  success = false
end

if success
  create_makefile 'redis'
else
  puts
  puts "ERROR: Not all dependencies were met."
end
