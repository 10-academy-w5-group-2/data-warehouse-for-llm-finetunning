import React, {useState} from 'react';

//import components
import Logo from '../assets/img/logo.png';

//import icons
import { FaBars } from 'react-icons/fa';
import { BsArrowRight } from 'react-icons/bs';


const Header = () => {
  return (
  <header className='mb-12 lg:mb-0 z-20 relative px-4 lg:px-0'>
    <div className='container mx-auto'>
      <div className='flex items-center justify-between'>
        <div className='flex items-center gap-x-[110px]'>

          <a href="#">
            <img src={Logo} alt="" />
          </a>
        </div>
        
        {/*button */}
        <button className='btn btn-quaternary flex items-center gap-x-[20px] group'>
          Dashboard <BsArrowRight className='text-2x1 text-accent-primary group-hover:text-white transition'/>
        </button>

        <div className='lg:hidden text-2x1 text-primary cursor-pointer'>
          <FaBars />
        </div>
      </div>
    </div>
    </header>
  );
};

export default Header;
